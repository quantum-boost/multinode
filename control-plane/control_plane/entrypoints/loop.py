import logging
import signal
from types import FrameType
from typing import Optional

from control_plane.control.periodic.all import LifecycleActions
from control_plane.data.data_store import DataStore
from control_plane.entrypoints.utils.cli_arguments import parse_cli_arguments
from control_plane.entrypoints.utils.current_time import current_time, pause
from control_plane.entrypoints.utils.provisioner_setup import (
    provisioner_from_environment_variables,
)
from control_plane.entrypoints.utils.sql_setup import (
    datastore_from_environment_variables,
)
from control_plane.provisioning.provisioner import AbstractProvisioner

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class LoopInterruptedException(Exception):
    pass


def signal_handler(sig: int, frame: Optional[FrameType]) -> None:
    if sig == signal.SIGINT or sig == signal.SIGTERM:
        raise LoopInterruptedException


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def run_loop(data_store: DataStore, provisioner: AbstractProvisioner) -> None:
    loop_actions = LifecycleActions(data_store, provisioner)

    while True:
        try:
            loop_actions.run_once(current_time())
            pause(seconds=1)
        except LoopInterruptedException:
            logging.warning("Control loop interrupted")
            break
        except Exception:
            logging.error("Unexpected error in control loop", exc_info=True)
            continue


def main() -> None:
    cli_args = parse_cli_arguments()
    provisioner = provisioner_from_environment_variables(cli_args)

    with datastore_from_environment_variables(cli_args) as data_store:
        run_loop(data_store, provisioner)


if __name__ == "__main__":
    main()
