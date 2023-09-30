import argparse
import logging

from control_plane.provisioning.dev_provisioner import DevelopmentProvisioner
from control_plane.provisioning.provisioner import AbstractProvisioner

DEV_PROVISIONER_CLI_VALUE = "dev"


def provisioner_from_cli_arguments() -> AbstractProvisioner:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--provisioner", type=str, help="Type of provisioner", choices=[DEV_PROVISIONER_CLI_VALUE], required=True
    )

    args = parser.parse_args()

    if args.provisioner == DEV_PROVISIONER_CLI_VALUE:
        logging.info("Using dev provisioner")
        return DevelopmentProvisioner(lag_cycles=5)
    else:
        raise ValueError
