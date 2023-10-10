import logging

from control_plane.entrypoints.utils.authenticator_setup import (
    CONTROL_PLANE_API_KEY_ENV,
)
from control_plane.entrypoints.utils.cli_arguments import CliArguments, ProvisionerType
from control_plane.entrypoints.utils.environment import (
    get_mandatory_environment_variable,
)
from control_plane.provisioning.dev_provisioner import DevelopmentProvisioner
from control_plane.provisioning.ecs_provisioner import EcsProvisioner
from control_plane.provisioning.provisioner import AbstractProvisioner

PROVISIONER_API_URL_ENV = "PROVISIONER_API_URL"
PROVISIONER_API_KEY_ENV = "PROVISIONER_API_KEY"
CONTROL_PLANE_API_URL_ENV = "CONTROL_PLANE_API_URL"
AWS_REGION_ENV = "AWS_REGION"
CLUSTER_NAME_ENV = "CLUSTER_NAME"
SUBNET_IDS_ENV = "SUBNET_IDS"
SECURITY_GROUP_IDS_ENV = "SECURITY_GROUP_IDS"
ASSIGN_PUBLIC_IP_ENV = "ASSIGN_PUBLIC_IP"
TASK_ROLE_ARN_ENV = "TASK_ROLE_ARN"
EXECUTION_ROLE_ARN_ENV = "EXECUTION_ROLE_ARN"
LOG_GROUP_ENV = "LOG_GROUP"


def provisioner_from_environment_variables(
    cli_args: CliArguments,
) -> AbstractProvisioner:
    if cli_args.provisioner_type == ProvisionerType.DEV:
        logging.info("Using dev provisioner")
        return DevelopmentProvisioner(lag_cycles=5)

    elif cli_args.provisioner_type == ProvisionerType.ECS:
        logging.info("Using ECS provisioner")
        return EcsProvisioner(
            control_plane_api_url=get_mandatory_environment_variable(
                CONTROL_PLANE_API_URL_ENV
            ),
            control_plane_api_key=get_mandatory_environment_variable(
                CONTROL_PLANE_API_KEY_ENV
            ),
            aws_region=get_mandatory_environment_variable(AWS_REGION_ENV),
            cluster_name=get_mandatory_environment_variable(CLUSTER_NAME_ENV),
            subnet_ids=get_mandatory_environment_variable(SUBNET_IDS_ENV).split(","),
            security_group_ids=get_mandatory_environment_variable(
                SECURITY_GROUP_IDS_ENV
            ).split(","),
            assign_public_ip=bool(
                get_mandatory_environment_variable(ASSIGN_PUBLIC_IP_ENV)
            ),
            task_role_arn=get_mandatory_environment_variable(TASK_ROLE_ARN_ENV),
            execution_role_arn=get_mandatory_environment_variable(
                EXECUTION_ROLE_ARN_ENV
            ),
            log_group=get_mandatory_environment_variable(LOG_GROUP_ENV),
        )

    else:
        raise ValueError
