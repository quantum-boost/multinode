import logging

from control_plane.docker.credentials_loader import (
    AbstractContainerRepositoryCredentialsLoader,
)
from control_plane.docker.ecr_credentials_loader import (
    EcrContainerRepositoryCredentialsLoader,
)
from control_plane.entrypoints.utils.cli_arguments import CliArguments, ProvisionerType
from control_plane.entrypoints.utils.environment import (
    get_mandatory_environment_variable,
)
from control_plane.entrypoints.utils.provisioner_setup import AWS_REGION_ENV

CONTAINER_REPOSITORY_NAME_ENV = "CONTAINER_REPOSITORY_NAME"


def credentials_loader_from_environment_variables(
    cli_args: CliArguments,
) -> AbstractContainerRepositoryCredentialsLoader:
    if cli_args.provisioner_type == ProvisionerType.ECS:
        logging.info("Using ECR container repository credentials loader")
        return EcrContainerRepositoryCredentialsLoader(
            repository_name=get_mandatory_environment_variable(
                CONTAINER_REPOSITORY_NAME_ENV
            ),
            aws_region=get_mandatory_environment_variable(AWS_REGION_ENV),
        )

    else:
        raise ValueError
