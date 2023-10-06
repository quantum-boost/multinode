import argparse
import json

from control_plane.data.data_store import DataStore
from control_plane.entrypoints.api_setup.api_endpoints import build_app
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
        "--output",
        type=str,
        help="Output file path for the schema",
        required=True,
    )
    output = parser.parse_args().output

    # Disable abstract methods since we don't have to implement them in fake classes
    # Also disable mypy because it's not happy about it
    FakeProvisioner.__abstractmethods__ = set()  # type: ignore
    provisioner = FakeProvisioner()  # type: ignore

    FakeAuthenticator.__abstractmethods__ = set()  # type: ignore
    authenticator = FakeAuthenticator()  # type: ignore

    data_store = FakeDataStore()
    app = build_app(data_store, provisioner, authenticator)
    with open(output, "w") as f:
        json.dump(app.openapi(), f, indent=2)
