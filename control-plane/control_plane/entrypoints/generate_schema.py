import argparse
import json

from control_plane.data.data_store import DataStore
from control_plane.entrypoints.api_setup.api_endpoints import build_app
from control_plane.entrypoints.utils.documentation import document_all_errors
from control_plane.provisioning.provisioner import AbstractProvisioner
from control_plane.user_management.authenticator import AbstractAuthenticator


class FakeProvisioner(AbstractProvisioner):
    pass


class FakeDataStore(DataStore):
    def __init__(self) -> None:
        pass


class FakeAuthenticator(AbstractAuthenticator):
    pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--schema-output",
        type=str,
        help="Output file path for the schema",
        required=True,
    )

    parser.add_argument(
        "--error-types-output",
        type=str,
        help="Output file path for the error types",
        required=True,
    )

    args = parser.parse_args()

    schema_output = args.schema_output
    error_types_output = args.error_types_output

    # Disable abstract methods since we don't have to implement them in fake classes
    # Also disable mypy because it's not happy about it
    FakeProvisioner.__abstractmethods__ = set()  # type: ignore
    provisioner = FakeProvisioner()  # type: ignore

    FakeAuthenticator.__abstractmethods__ = set()  # type: ignore
    authenticator = FakeAuthenticator()  # type: ignore

    data_store = FakeDataStore()
    app = build_app(data_store, provisioner, authenticator)
    with open(schema_output, "w") as f:
        json.dump(app.openapi(), f, indent=2)

    with open(error_types_output, "w") as f:
        json.dump(document_all_errors(), f, indent=2)
