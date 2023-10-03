from typing import Optional, NamedTuple

import boto3
from botocore.exceptions import ClientError

from control_plane.provisioning.provisioner import AbstractProvisioner, LogsResult
from control_plane.types.datatypes import ResourceSpec, PreparedFunctionDetails, WorkerDetails, WorkerType, WorkerStatus

CONTAINER_NAME = "main"
LOG_STREAM_PREFIX = "ecs"

PROJECT_NAME_ENV = "PROJECT_NAME"
VERSION_ID_ENV = "VERSION_ID"
FUNCTION_NAME_ENV = "FUNCTION_NAME"
INVOCATION_ID_ENV = "INVOCATION_ID"
EXECUTION_ID_ENV = "EXECUTION_ID"
CONTROL_API_URL_ENV = "CONTROL_API_URL"
API_KEY_ENV = "API_KEY"

# Prefect saves API keys as an environment variable in ECS tasks.
# I don't think this is the most secure option, since the environment variable is visible in the console.
# We should instead use Amazon's secrets manager.
# But it will do for now.

# This is the highest we can use with Fargate
TERMINATION_GRACE_PERIOD_SECONDS = 120

# Hardcoded for now - see TODO later
MAX_LOG_LINES = 100


class EcsProvisioner(AbstractProvisioner):
    def __init__(
        self,
        *,
        region: str,
        cluster_name: str,
        subnet_ids: list[str],
        security_group_ids: list[str],
        assign_public_ip: bool,
        task_role_arn: str,
        execution_role_arn: str,
        log_group: str,
        control_api_url: str,
        api_key: str,
    ):
        self._ecs_client = boto3.client("ecs", region_name=region)
        self._logs_client = boto3.client("logs", region_name=region)

        self._region = region
        self._cluster_name = cluster_name
        self._subnet_ids = subnet_ids
        self._security_group_ids = security_group_ids
        self._assign_public_ip = assign_public_ip
        self._task_role_arn = task_role_arn
        self._execution_role_arn = execution_role_arn
        self._log_group = log_group
        self._control_api_url = control_api_url
        self._api_key = api_key

    def prepare_function(
        self, *, project_name: str, version_id: str, function_name: str, docker_image: str, resource_spec: ResourceSpec
    ) -> PreparedFunctionDetails:
        # Don't need to be too careful to avoid clashes - AWS assigns different version numbers in case of clashes
        # Better to keep this short
        family_name = f"{project_name[:16]}-{function_name[:16]}"

        cpu_memory_units = select_cpu_memory_units(resource_spec.virtual_cpus, resource_spec.memory_gbs)

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
                        {"name": CONTROL_API_URL_ENV, "value": self._control_api_url},
                        {"name": API_KEY_ENV, "value": self._api_key},
                    ],
                    "stopTimeout": TERMINATION_GRACE_PERIOD_SECONDS,
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-region": self._region,
                            "awslogs-group": self._log_group,
                            "awslogs-stream-prefix": LOG_STREAM_PREFIX,
                        },
                    },
                },
            ],
        )

        task_definition_arn = response["taskDefinition"]["taskDefinitionArn"]
        assert isinstance(task_definition_arn, str)

        return PreparedFunctionDetails(type=WorkerType.AWS_ECS, identifier=task_definition_arn)

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
        response = self._ecs_client.run_task(
            cluster=self._cluster_name,
            taskDefinition=prepared_function_details.identifier,
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": self._subnet_ids,
                    "securityGroups": self._security_group_ids,
                    "assignPublicIp": "ENABLED" if self._assign_public_ip else "DISABLED",
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

        log_stream_name = f"{LOG_STREAM_PREFIX}/{CONTAINER_NAME}/{task_arn.split('/')[-1]}"

        return WorkerDetails(type=WorkerType.AWS_ECS, identifier=task_arn, logs_identifier=log_stream_name)

    def send_termination_signal_to_worker(self, *, worker_details: WorkerDetails) -> None:
        self._ecs_client.stop_task(cluster=self._cluster_name, task=worker_details.identifier)

    def check_worker_status(self, *, worker_details: WorkerDetails) -> WorkerStatus:
        response = self._ecs_client.describe_tasks(cluster=self._cluster_name, tasks=[worker_details.identifier])

        if len(response["tasks"]) == 1:
            assert "lastStatus" in response["tasks"][0]
            ecs_task_status = response["tasks"][0]["lastStatus"]
            if ecs_task_status != "STOPPED":
                return WorkerStatus.RUNNING
            else:
                return WorkerStatus.TERMINATED
        else:
            # If the task terminated more than a few hours ago, then the task will have been deleted altogether.
            assert len(response["failures"]) == 1
            assert "reason" in response["failures"][0]
            assert response["failures"][0]["reason"] == "MISSING"
            return WorkerStatus.TERMINATED

    def get_worker_logs(
        self, *, worker_details: WorkerDetails, max_lines: Optional[int], initial_offset: Optional[str]
    ) -> LogsResult:
        # TODO: Unfortunately, the pagination of logs is unintuitive. I've seen this problem before.
        # For now, we'll hardcode max_lines = 100 and initial_offset = 0
        try:
            response = self._logs_client.get_log_events(
                logGroupName=self._log_group,
                logStreamName=worker_details.logs_identifier,
                limit=MAX_LOG_LINES,
                startFromHead=True,
            )

            log_lines = [event["message"] for event in response["events"]]

            return LogsResult(log_lines=log_lines, next_offset=None)
        except ClientError as ex:
            error_code = ex.response["Error"]["Code"]
            if error_code == "ResourceNotFoundException":
                return LogsResult(log_lines=[], next_offset=None)
            else:
                raise ex


class EcsCpuMemoryUnits(NamedTuple):
    cpu_units: int  # NB 1 virtual CPU is 1024 of these CPU units
    memory_units: int  # 1 memory GB is 1024 of these memory units


# The ordering of this list in important - we pick the *first* item that is big enough for the user's requirements.
# TODO: This is not a complete list. But it's enough to get going with.
ALLOWED_CPU_MEMORY_OPTIONS: list[EcsCpuMemoryUnits] = [
    EcsCpuMemoryUnits(cpu_units=256, memory_units=512),
    EcsCpuMemoryUnits(cpu_units=256, memory_units=1024),
    EcsCpuMemoryUnits(cpu_units=256, memory_units=2048),
    EcsCpuMemoryUnits(cpu_units=512, memory_units=1024),
    EcsCpuMemoryUnits(cpu_units=512, memory_units=2048),
    EcsCpuMemoryUnits(cpu_units=512, memory_units=4096),
    EcsCpuMemoryUnits(cpu_units=1024, memory_units=2048),
    EcsCpuMemoryUnits(cpu_units=1024, memory_units=4096),
    EcsCpuMemoryUnits(cpu_units=1024, memory_units=8192),
    EcsCpuMemoryUnits(cpu_units=2048, memory_units=4096),
    EcsCpuMemoryUnits(cpu_units=2048, memory_units=8192),
    EcsCpuMemoryUnits(cpu_units=2048, memory_units=16384),
    EcsCpuMemoryUnits(cpu_units=4096, memory_units=8192),
    EcsCpuMemoryUnits(cpu_units=4096, memory_units=16384),
    EcsCpuMemoryUnits(cpu_units=8192, memory_units=16384),
    EcsCpuMemoryUnits(cpu_units=8192, memory_units=32768),
    EcsCpuMemoryUnits(cpu_units=16384, memory_units=32768),
    EcsCpuMemoryUnits(cpu_units=16384, memory_units=65536),
]

EPSILON = 1.0e-5


def select_cpu_memory_units(desired_virtual_cpus: float, desired_memory_gbs: float) -> EcsCpuMemoryUnits:
    for option in ALLOWED_CPU_MEMORY_OPTIONS:
        enough_cpu = (option.cpu_units / (1024.0 * desired_virtual_cpus)) > 1.0 - EPSILON
        enough_memory = (option.memory_units / (1024.0 * desired_memory_gbs)) > 1.0 - EPSILON
        if enough_cpu and enough_memory:
            return option

    # In case of rounding errors at the top end, return the most expensive option.
    return ALLOWED_CPU_MEMORY_OPTIONS[-1]
