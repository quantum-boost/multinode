import logging

from control_plane.entrypoints.utils.authenticator_setup import API_KEY_ENV
from control_plane.entrypoints.utils.cli_arguments import CliArguments
from control_plane.entrypoints.utils.environment import get_mandatory_environment_variable
from control_plane.provisioning.dev_provisioner import DevelopmentProvisioner
from control_plane.provisioning.ecs_provisioner import EcsProvisioner
from control_plane.provisioning.provisioner import AbstractProvisioner
from control_plane.types.datatypes import WorkerType

AWS_ACCESS_KEY_ID_ENV = "AWS_ACCESS_KEY_ID"  # boto3 looks for this automatically, so we don't explicitly touch it
AWS_SECRET_ACCESS_KEY_ENV = "AWS_SECRET_ACCESS_KEY"  # same as above
AWS_DEFAULT_REGION_ENV = "AWS_DEFAULT_REGION"  # go with AWS's convention for this environment variable name
CLUSTER_NAME_ENV = "CLUSTER_NAME"
SUBNET_IDS_ENV = "SUBNET_IDS"
SECURITY_GROUP_IDS_ENV = "SECURITY_GROUP_IDS"
ASSIGN_PUBLIC_IP_ENV = "ASSIGN_PUBLIC_IP"
TASK_ROLE_ARN_ENV = "TASK_ROLE_ARN"
EXECUTION_ROLE_ARN_ENV = "EXECUTION_ROLE_ARN"
LOG_GROUP_ENV = "LOG_GROUP"
CONTROL_PLANE_URL_ENV = "CONTROL_PLANE_URL"


def provisioner_from_environment_variables(cli_args: CliArguments) -> AbstractProvisioner:
    if cli_args.worker_type == WorkerType.TEST:
        logging.info("Using dev provisioner")
        return DevelopmentProvisioner(lag_cycles=5)

    elif cli_args.worker_type == WorkerType.AWS_ECS:
        logging.info("Using ECS provisioner")
        return EcsProvisioner(
            region=get_mandatory_environment_variable(AWS_DEFAULT_REGION_ENV),
            cluster_name=get_mandatory_environment_variable(CLUSTER_NAME_ENV),
            subnet_ids=get_mandatory_environment_variable(SUBNET_IDS_ENV).split(","),
            security_group_ids=get_mandatory_environment_variable(SECURITY_GROUP_IDS_ENV).split(","),
            assign_public_ip=bool(get_mandatory_environment_variable(ASSIGN_PUBLIC_IP_ENV)),
            task_role_arn=get_mandatory_environment_variable(TASK_ROLE_ARN_ENV),
            execution_role_arn=get_mandatory_environment_variable(EXECUTION_ROLE_ARN_ENV),
            log_group=get_mandatory_environment_variable(LOG_GROUP_ENV),
            control_api_url=get_mandatory_environment_variable(CONTROL_PLANE_URL_ENV),
            api_key=get_mandatory_environment_variable(API_KEY_ENV),
        )

    else:
        raise ValueError
