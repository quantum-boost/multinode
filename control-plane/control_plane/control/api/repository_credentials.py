from control_plane.docker.credentials_loader import (
    AbstractContainerRepositoryCredentialsLoader,
)
from control_plane.types.datatypes import ContainerRepositoryCredentials


class ContainerRepositoryCredentialsApiHandler:
    """
    API method for getting credentials to container repository
    """

    def __init__(
        self, credentials_loader: AbstractContainerRepositoryCredentialsLoader
    ) -> None:
        self._credentials_loader = credentials_loader

    def get_container_repository_credentials(self) -> ContainerRepositoryCredentials:
        return self._credentials_loader.load()
