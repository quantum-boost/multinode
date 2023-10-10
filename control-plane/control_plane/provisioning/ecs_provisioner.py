from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from control_plane.provisioning.ecs_cpu_memory_helper import select_cpu_memory_units
from control_plane.provisioning.provisioner import AbstractProvisioner, LogsResult
from control_plane.types.datatypes import (
    PreparedFunctionDetails,
    ResourceSpec,
    WorkerDetails,
    WorkerStatus,
    WorkerType,
)

# Constants
CONTAINER_NAME = "main"
LOG_STREAM_PREFIX = "ecs"
TERMINATION_GRACE_PERIOD_SECONDS = 120

# Environment variables that this provisioner will inject into the ECS tasks that it will create
PROJECT_NAME_ENV = "PROJECT_NAME"
VERSION_ID_ENV = "VERSION_ID"
FUNCTION_NAME_ENV = "FUNCTION_NAME"
INVOCATION_ID_ENV = "INVOCATION_ID"
EXECUTION_ID_ENV = "EXECUTION_ID"
CONTROL_PLANE_API_URL_ENV = "CONTROL_PLANE_API_URL"
CONTROL_PLANE_API_KEY_ENV = "CONTROL_PLANE_API_KEY"


class EcsProvisioner(AbstractProvisioner):
    def __init__(
        self,
        control_plane_api_url: str,
        control_plane_api_key: str,
        aws_region: str,
        cluster_name: str,
        subnet_ids: list[str],
        security_group_ids: list[str],
        assign_public_ip: bool,
        task_role_arn: str,
        execution_role_arn: str,
        log_group: str,
    ):
        self._ecs_client = boto3.client("ecs", config=Config(region_name=aws_region))
        self._logs_client = boto3.client("logs", config=Config(region_name=aws_region))

        self._control_plane_api_url = control_plane_api_url
        self._control_plane_api_key = control_plane_api_key
        self._aws_region = aws_region
        self._cluster_name = cluster_name
        self._subnet_ids = subnet_ids
        self._security_group_ids = security_group_ids
        self._assign_public_ip = assign_public_ip
        self._task_role_arn = task_role_arn
        self._execution_role_arn = execution_role_arn
        self._log_group = log_group

    def prepare_function(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        docker_image: str,
        resource_spec: ResourceSpec,
    ) -> PreparedFunctionDetails:
        # Don't need to be too careful to avoid clashes between task definition family names.
        # AWS assigns different version numbers to task definitions in case of such clashes.
        # We prefer to keep these family names short, since AWS may impose limits on length
        family_name = f"multinode-worker-{project_name[:16]}-{function_name[:16]}"

        cpu_memory_units = select_cpu_memory_units(
            resource_spec.virtual_cpus, resource_spec.memory_gbs
        )

        response = self._ecs_client.register_task_definition(
            family=family_name,
            taskRoleArn=self._task_role_arn,
            executionRoleArn=self._execution_role_arn,
            networkMode="awsvpc",
            requiresCompatibilities=[
                "FARGATE",
            ],
            cpu=str(cpu_memory_units.cpu_units),
            memory=str(cpu_memory_units.memory_units),
            containerDefinitions=[
                {
                    "name": CONTAINER_NAME,
                    "image": docker_image,
                    "environment": [
                        {"name": PROJECT_NAME_ENV, "value": project_name},
                        {"name": VERSION_ID_ENV, "value": version_id},
                        {"name": FUNCTION_NAME_ENV, "value": function_name},
                        {
                            "name": CONTROL_PLANE_API_URL_ENV,
                            "value": self._control_plane_api_url,
                        },
                        {
                            "name": CONTROL_PLANE_API_KEY_ENV,
                            "value": self._control_plane_api_key,
                        },
                    ],
                    "stopTimeout": TERMINATION_GRACE_PERIOD_SECONDS,
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-region": self._aws_region,
                            "awslogs-group": self._log_group,
                            "awslogs-stream-prefix": LOG_STREAM_PREFIX,
                        },
                    },
                },
            ],
        )

        task_definition_arn = response["taskDefinition"]["taskDefinitionArn"]

        return PreparedFunctionDetails(
            type=WorkerType.AWS_ECS, identifier=task_definition_arn
        )

    def provision_worker(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        resource_spec: ResourceSpec,
        prepared_function_details: PreparedFunctionDetails,
    ) -> WorkerDetails:
        task_definition_arn = prepared_function_details.identifier

        response = self._ecs_client.run_task(
            cluster=self._cluster_name,
            taskDefinition=task_definition_arn,
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": self._subnet_ids,
                    "securityGroups": self._security_group_ids,
                    "assignPublicIp": "ENABLED"
                    if self._assign_public_ip
                    else "DISABLED",
                },
            },
            overrides={
                "containerOverrides": [
                    {
                        "name": CONTAINER_NAME,
                        "environment": [
                            {"name": INVOCATION_ID_ENV, "value": invocation_id},
                            {"name": EXECUTION_ID_ENV, "value": execution_id},
                        ],
                    }
                ]
            },
        )

        task_arn = response["tasks"][0]["taskArn"]
        assert isinstance(task_arn, str)

        log_stream_name = (
            f"{LOG_STREAM_PREFIX}/{CONTAINER_NAME}/{task_arn.split('/')[-1]}"
        )

        return WorkerDetails(
            type=WorkerType.AWS_ECS,
            identifier=task_arn,
            logs_identifier=log_stream_name,
        )

    def send_termination_signal_to_worker(
        self, *, worker_details: WorkerDetails
    ) -> None:
        task_arn = worker_details.identifier

        self._ecs_client.stop_task(cluster=self._cluster_name, task=task_arn)

    def check_worker_status(self, *, worker_details: WorkerDetails) -> WorkerStatus:
        task_arn = worker_details.identifier

        response = self._ecs_client.describe_tasks(
            cluster=self._cluster_name, tasks=[task_arn]
        )

        if len(response["tasks"]) == 1:
            ecs_task_status = response["tasks"][0]["lastStatus"]
            if ecs_task_status != "STOPPED":
                status = WorkerStatus.RUNNING
            else:
                status = WorkerStatus.TERMINATED
        else:
            # If the task terminated more than a few hours ago, then the task will have been deleted altogether.
            # This *also* counts as the task being in TERMINATED status.
            assert len(response["failures"]) == 1
            assert response["failures"][0]["reason"] == "MISSING"
            status = WorkerStatus.TERMINATED

        return status

    def get_worker_logs(
        self,
        *,
        worker_details: WorkerDetails,
        max_lines: int,
        initial_offset: Optional[str],
    ) -> LogsResult:
        log_stream_name = worker_details.logs_identifier

        try:
            if initial_offset is None:
                response = self._logs_client.get_log_events(
                    logGroupName=self._log_group,
                    logStreamName=log_stream_name,
                    limit=max_lines,
                    startFromHead=True,
                )
            else:
                response = self._logs_client.get_log_events(
                    logGroupName=self._log_group,
                    logStreamName=log_stream_name,
                    limit=max_lines,
                    startFromHead=True,
                    nextToken=initial_offset,
                )

            log_lines = [event["message"] for event in response["events"]]

            # Note: If there are currently no more logs in the log stream, then next_offset will be equal to
            # initial_offset, not None! Remember, it's possible for more logs to be generated in the future,
            # and therefore, it's possible that a future .get_log_events call using this same offset will
            # return something.
            next_offset = response["nextForwardToken"]

            return LogsResult(log_lines=log_lines, next_offset=next_offset)

        except ClientError as ex:
            error_code = ex.response["Error"]["Code"]
            if error_code == "ResourceNotFoundException":
                # Log stream not yet created => has no log lines
                return LogsResult(log_lines=[], next_offset=None)
            else:
                raise ex
