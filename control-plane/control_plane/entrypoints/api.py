import logging

import uvicorn

from control_plane.entrypoints.api_setup.api_endpoints import build_app
from control_plane.entrypoints.utils.authenticator_setup import (
    authenticator_from_environment_variables,
)
from control_plane.entrypoints.utils.cli_arguments import parse_cli_arguments
from control_plane.entrypoints.utils.provisioner_setup import (
    provisioner_from_environment_variables,
)
from control_plane.entrypoints.utils.sql_setup import (
    datastore_from_environment_variables,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main() -> None:
    cli_args = parse_cli_arguments()
    provisioner = provisioner_from_environment_variables(cli_args)
    authenticator = authenticator_from_environment_variables()

    with datastore_from_environment_variables(cli_args) as data_store:
        app = build_app(data_store, provisioner, authenticator)
        uvicorn.run(app, host="0.0.0.0", port=5000)


if __name__ == "__main__":
    main()
