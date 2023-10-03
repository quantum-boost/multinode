import argparse
from enum import StrEnum
from typing import NamedTuple


class ProvisionerType(StrEnum):
    DEV = "dev"
    EXTERNAL = "external"


class CliArguments(NamedTuple):
    provisioner_type: ProvisionerType
    create_tables_at_start: bool
    delete_tables_at_end: bool


def parse_cli_arguments() -> CliArguments:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--provisioner",
        type=str,
        help="Type of provisioner",
        choices=[str(ProvisionerType.DEV), str(ProvisionerType.EXTERNAL)],
        required=True,
    )

    parser.add_argument(
        "--create-tables", action="store_true", help="Create database tables when the control loop starts"
    )

    parser.add_argument(
        "--delete-tables", action="store_true", help="Delete database tables when the control loop finishes"
    )

    args = parser.parse_args()

    provisioner_type = ProvisionerType(args.provisioner)

    create_tables_at_start = args.create_tables
    delete_tables_at_end = args.delete_tables

    return CliArguments(
        provisioner_type=provisioner_type,
        create_tables_at_start=create_tables_at_start,
        delete_tables_at_end=delete_tables_at_end,
    )
