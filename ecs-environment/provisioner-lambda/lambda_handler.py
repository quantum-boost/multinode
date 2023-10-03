from json import JSONDecodeError

import boto3
from botocore.exceptions import ClientError, BotoCoreError
import json
import os
from typing import NamedTuple

# Constants
CONTAINER_NAME = "main"
LOG_STREAM_PREFIX = "ecs"
TERMINATION_GRACE_PERIOD_SECONDS = 120

# Request/response body keys
PROJECT_NAME_KEY = "project_name"
VERSION_ID_KEY = "version_id"
FUNCTION_NAME_KEY = "function_name"
INVOCATION_ID_KEY = "invocation_id"
EXECUTION_ID_KEY = "execution_id"

DOCKER_IMAGE_KEY = "docker_image"

RESOURCE_SPEC_KEY = "resource_spec"
VIRTUAL_CPUS_KEY = "virtual_cpus"
MEMORY_GBS_KEY = "memory_gbs"

PREPARED_FUNCTION_DETAILS_KEY = "prepared_function_details"
WORKER_DETAILS_KEY = "worker_details"
IDENTIFIER_KEY = "identifier"
LOGS_IDENTIFIER_KEY = "logs_identifier"

TYPE_KEY = "type"
WORKER_STATUS_KEY = "worker_status"

LOG_LINES_KEY = "log_lines"
MAX_LINES_KEY = "max_lines"
INITIAL_OFFSET_KEY = "initial_offset"
NEXT_OFFSET_KEY = "next_offset"

# Request/response enum values
AWS_ECS_TYPE = "AWS_ECS"
RUNNING_STATUS = "RUNNING"
TERMINATED_STATUS = "TERMINATED"

# Environment variables used within this lambda itself
PROVISIONER_API_KEY_ENV = "PROVISIONER_API_KEY"
CONTROL_PLANE_API_URL_ENV = "CONTROL_PLANE_API_URL"
CONTROL_PLANE_API_KEY_ENV = "CONTROL_PLANE_API_KEY"
AWS_REGION_ENV = "AWS_REGION"
CLUSTER_NAME_ENV = "CLUSTER_NAME"
SUBNET_IDS_ENV = "SUBNET_IDS"
SECURITY_GROUP_IDS_ENV = "SECURITY_GROUP_IDS"
ASSIGN_PUBLIC_IP_ENV = "ASSIGN_PUBLIC_IP"
TASK_ROLE_ARN_ENV = "TASK_ROLE_ARN"
EXECUTION_ROLE_ARN_ENV = "EXECUTION_ROLE_ARN"
LOG_GROUP_ENV = "LOG_GROUP"

# Environment variables that this lambda will inject into the ECS tasks that it will create
PROJECT_NAME_ENV = "PROJECT_NAME"
VERSION_ID_ENV = "VERSION_ID"
FUNCTION_NAME_ENV = "FUNCTION_NAME"
INVOCATION_ID_ENV = "INVOCATION_ID"
EXECUTION_ID_ENV = "EXECUTION_ID"

# URL paths
PREPARE_PATH = "/prepare"
PROVISION_PATH = "/provision"
TERMINATE_PATH = "/terminate"
CHECK_STATUS_PATH = "/check_status"
GET_LOGS_PATH = "/get_logs"

ecs_client = boto3.client("ecs")
logs_client = boto3.client("logs")


def lambda_handler(event, context):
    if not has_valid_auth_header(event):
        return {
            "statusCode": 403,
            "body": {"detail": "API key is invalid"}
        }

    http_method = event.get("requestContext", {}).get("http", {}).get("method")
    if http_method != "POST":
        return {
            "statusCode": 405,
            "body": {"detail": "Method not allowed"}
        }

    # Can't hoist to top level, since the values are functions.
    paths_to_handler_functions = {
        PREPARE_PATH: prepare_function,
        PROVISION_PATH: provision_task,
        TERMINATE_PATH: terminate_task,
        CHECK_STATUS_PATH: check_task_status,
        GET_LOGS_PATH: get_task_logs
    }

    url_path = event.get("requestContext", {}).get("http", {}).get("path")
    handler_function = paths_to_handler_functions.get(url_path)

    if handler_function is None:
        return {
            "statusCode": 404,
            "body": {"detail": "Action does not exist"}
        }

    if http_method != "POST":
        return {
            "statusCode": 405,
            "body": {"detail": "HTTP method must be POST"}
        }

    try:
        request_body = json.loads(event.get("body", {}))
    except JSONDecodeError as ex:
        return {
            "statusCode": 422,
            "body": {"detail": str(ex)}
        }

    try:
        response_body = handler_function(request_body)
        return {
            "statusCode": 200,
            "body": response_body
        }
    except RequestFormattingError as ex:
        return {
            "statusCode": 422,
            "body": {"detail": str(ex)}
        }
    except (ClientError, BotoCoreError) as ex:
        # Not ideal to always return a 400.
        # Would ideally return a 500 if the error is AWS's fault (e.g. temporary outage).
        # But it's hard to tell which errors are AWS's fault vs the client's fault.
        return {
            "statusCode": 400,
            "body": {"detail": ex.response.get("Error", {}).get("Code", "")}
        }


class RequestFormattingError(BaseException):
    pass


def get_field(request_body, field_name, allowed_types):
    if not isinstance(request_body, dict):
        # We'll never actually hit this path, since an error would already have been thrown at
        # a higher level of nesting.
        raise RequestFormattingError(f"Request body is missing a level of nesting")

    if field_name not in request_body and None not in allowed_types:
        raise RequestFormattingError(f"Field {field_name} is mandatory")

    value = request_body.get(field_name)

    if not (
        (value is None and None in allowed_types)
        or any(isinstance(value, allowed_type) for allowed_type in allowed_types if allowed_type is not None)
    ):
        allowed_types_str = ', '.join(str(allowed_type) for allowed_type in allowed_types)
        raise RequestFormattingError(f"Field {field_name}: allowed types are {allowed_types_str}")

    return value


def get_environment_variable(variable_name):
    return os.environ[variable_name]


def has_valid_auth_header(event):
    auth_header = event.get("headers", {}).get("authorization")

    token = None
    if auth_header.startswith("Bearer "):
        token = auth_header[len("Bearer "):]

    return token == get_environment_variable(PROVISIONER_API_KEY_ENV)


def prepare_function(request_body):
    project_name = get_field(request_body, PROJECT_NAME_KEY, [str])
    version_id = get_field(request_body, VERSION_ID_KEY, [str])
    function_name = get_field(request_body, FUNCTION_NAME_KEY, [str])

    # Don't need to be too careful to avoid clashes between task definition family names.
    # AWS assigns different version numbers to task definitions in case of such clashes.
    # We prefer to keep these family names short, since AWS may impose limits on length
    family_name = f"multinode-worker-{project_name[:16]}-{function_name[:16]}"

    task_role_arn = get_environment_variable(TASK_ROLE_ARN_ENV)
    execution_role_arn = get_environment_variable(EXECUTION_ROLE_ARN_ENV)

    resource_spec = get_field(request_body, RESOURCE_SPEC_KEY, [dict])
    virtual_cpus = get_field(resource_spec, VIRTUAL_CPUS_KEY, [float, int])
    memory_gbs = get_field(resource_spec, MEMORY_GBS_KEY, [float, int])
    cpu_memory_units = select_cpu_memory_units(virtual_cpus, memory_gbs)

    docker_image = get_field(request_body, DOCKER_IMAGE_KEY, [str])

    control_plane_api_url = get_environment_variable(CONTROL_PLANE_API_URL_ENV)
    control_plane_api_key = get_environment_variable(CONTROL_PLANE_API_KEY_ENV)

    region = get_environment_variable(AWS_REGION_ENV)
    log_group = get_environment_variable(LOG_GROUP_ENV)

    response = ecs_client.register_task_definition(
        family=family_name,
        taskRoleArn=task_role_arn,
        executionRoleArn=execution_role_arn,
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
                    {"name": CONTROL_PLANE_API_URL_ENV, "value": control_plane_api_url},
                    {"name": CONTROL_PLANE_API_KEY_ENV, "value": control_plane_api_key},
                ],
                "stopTimeout": TERMINATION_GRACE_PERIOD_SECONDS,
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-region": region,
                        "awslogs-group": log_group,
                        "awslogs-stream-prefix": LOG_STREAM_PREFIX,
                    },
                },
            },
        ],
    )

    task_definition_arn = response["taskDefinition"]["taskDefinitionArn"]

    return {
        PREPARED_FUNCTION_DETAILS_KEY: {
            TYPE_KEY: AWS_ECS_TYPE,
            IDENTIFIER_KEY: task_definition_arn
        }
    }


def provision_task(request_body):
    cluster_name = get_environment_variable(CLUSTER_NAME_ENV)

    prepared_function_details = get_field(request_body, PREPARED_FUNCTION_DETAILS_KEY, [dict])
    task_definition_arn = get_field(prepared_function_details, IDENTIFIER_KEY, [str])

    subnet_ids = get_environment_variable(SUBNET_IDS_ENV).split(",")
    security_group_ids = get_environment_variable(SECURITY_GROUP_IDS_ENV).split(",")
    assign_public_ip = bool(get_environment_variable(ASSIGN_PUBLIC_IP_ENV))

    invocation_id = get_field(request_body, INVOCATION_ID_KEY, [str])
    execution_id = get_field(request_body, EXECUTION_ID_KEY, [str])

    response = ecs_client.run_task(
        cluster=cluster_name,
        taskDefinition=task_definition_arn,
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnet_ids,
                "securityGroups": security_group_ids,
                "assignPublicIp": "ENABLED" if assign_public_ip else "DISABLED",
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

    return {
        WORKER_DETAILS_KEY: {
            TYPE_KEY: AWS_ECS_TYPE,
            IDENTIFIER_KEY: task_arn,
            LOGS_IDENTIFIER_KEY: log_stream_name
        }
    }


def terminate_task(request_body):
    cluster_name = get_environment_variable(CLUSTER_NAME_ENV)

    worker_details = get_field(request_body, WORKER_DETAILS_KEY, [dict])
    task_arn = get_field(worker_details, IDENTIFIER_KEY, [str])

    ecs_client.stop_task(cluster=cluster_name, task=task_arn)

    return {}


def check_task_status(request_body):
    cluster_name = get_environment_variable(CLUSTER_NAME_ENV)

    worker_details = get_field(request_body, WORKER_DETAILS_KEY, [dict])
    task_arn = get_field(worker_details, IDENTIFIER_KEY, [str])

    response = ecs_client.describe_tasks(cluster=cluster_name, tasks=[task_arn])

    if len(response["tasks"]) == 1:
        ecs_task_status = response["tasks"][0]["lastStatus"]
        if ecs_task_status != "STOPPED":
            status = RUNNING_STATUS
        else:
            status = TERMINATED_STATUS
    else:
        # If the task terminated more than a few hours ago, then the task will have been deleted altogether.
        # This *also* counts as the task being in TERMINATED status.
        assert len(response["failures"]) == 1
        assert response["failures"][0]["reason"] == "MISSING"
        status = TERMINATED_STATUS

    return {
        WORKER_STATUS_KEY: status
    }


def get_task_logs(request_body):
    log_group = get_environment_variable(LOG_GROUP_ENV)
    worker_details = get_field(request_body, WORKER_DETAILS_KEY, [dict])
    log_stream_name = get_field(worker_details, LOGS_IDENTIFIER_KEY, [str])

    max_lines = get_field(request_body, MAX_LINES_KEY, [int])
    initial_offset = get_field(request_body, INITIAL_OFFSET_KEY, [str, None])

    try:
        if initial_offset is None:
            response = logs_client.get_log_events(
                logGroupName=log_group,
                logStreamName=log_stream_name,
                limit=max_lines,
                startFromHead=True,
            )
        else:
            response = logs_client.get_log_events(
                logGroupName=log_group,
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

        return {
            LOG_LINES_KEY: log_lines,
            NEXT_OFFSET_KEY: next_offset
        }

    except ClientError as ex:
        error_code = ex.response["Error"]["Code"]
        if error_code == "ResourceNotFoundException":
            # Log stream not yet created => has no log lines
            return {
                LOG_LINES_KEY: []
            }
        else:
            raise ex


class EcsCpuMemoryUnits(NamedTuple):
    cpu_units: int  # NB 1 virtual CPU is 1024 of these CPU units
    memory_units: int  # 1 memory GB is 1024 of these memory units


# The ordering of this list in important - we pick the *first* item that is big enough for the user's requirements.
# This is not a complete list. But it's enough to get going with.
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
