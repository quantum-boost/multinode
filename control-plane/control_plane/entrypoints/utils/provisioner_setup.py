import logging

from control_plane.entrypoints.utils.cli_arguments import CliArguments, ProvisionerType
from control_plane.entrypoints.utils.environment import get_mandatory_environment_variable
from control_plane.provisioning.dev_provisioner import DevelopmentProvisioner
from control_plane.provisioning.external_provisioner import ExternalProvisioner
from control_plane.provisioning.provisioner import AbstractProvisioner

PROVISIONER_API_URL_ENV = "PROVISIONER_API_URL"
PROVISIONER_API_KEY_ENV = "PROVISIONER_API_KEY"


def provisioner_from_environment_variables(cli_args: CliArguments) -> AbstractProvisioner:
    if cli_args.provisioner_type == ProvisionerType.DEV:
        logging.info("Using dev provisioner")
        return DevelopmentProvisioner(lag_cycles=5)

    elif cli_args.provisioner_type == ProvisionerType.EXTERNAL:
        logging.info("Using external provisioner")
        return ExternalProvisioner(
            provisioner_api_url=get_mandatory_environment_variable(PROVISIONER_API_URL_ENV),
            provisioner_api_key=get_mandatory_environment_variable(PROVISIONER_API_KEY_ENV),
        )

    else:
        raise ValueError
