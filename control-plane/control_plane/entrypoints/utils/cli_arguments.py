import argparse
from typing import NamedTuple

from control_plane.types.datatypes import WorkerType


DEV_PROVISIONER_CLI_VALUE = "dev"
ECS_PROVISIONER_CLI_VALUE = "ecs"


class CliArguments(NamedTuple):
    worker_type: WorkerType
    create_tables_at_start: bool
    delete_tables_at_end: bool


def parse_cli_arguments() -> CliArguments:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--provisioner",
        type=str,
        help="Type of provisioner",
        choices=[DEV_PROVISIONER_CLI_VALUE, ECS_PROVISIONER_CLI_VALUE],
        required=True,
    )

    parser.add_argument(
        "--create-tables", action="store_true", help="Create database tables when the control loop starts"
    )

    parser.add_argument(
        "--delete-tables", action="store_true", help="Delete database tables when the control loop finishes"
    )

    args = parser.parse_args()

    if args.provisioner == DEV_PROVISIONER_CLI_VALUE:
        worker_type = WorkerType.TEST
    elif args.provisioner == ECS_PROVISIONER_CLI_VALUE:
        worker_type = WorkerType.AWS_ECS
    else:
        raise ValueError

    create_tables_at_start = args.create_tables
    delete_tables_at_end = args.delete_tables

    return CliArguments(
        worker_type=worker_type,
        create_tables_at_start=create_tables_at_start,
        delete_tables_at_end=delete_tables_at_end,
    )
