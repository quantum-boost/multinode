from control_plane.docker.credentials_loader import (
    AbstractContainerRepositoryCredentialsLoader,
)
from control_plane.types.datatypes import ContainerRepositoryCredentials


class DummyContainerRepositoryCredentialsLoader(
    AbstractContainerRepositoryCredentialsLoader
):
    def load(self) -> ContainerRepositoryCredentials:
        return ContainerRepositoryCredentials(
            repository_name="mocked",
            username="mocked",
            password="mocked",
            endpoint_url="mocked",
        )
