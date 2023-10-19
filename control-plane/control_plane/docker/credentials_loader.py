from abc import ABC, abstractmethod

from control_plane.types.datatypes import ContainerRepositoryCredentials


class AbstractContainerRepositoryCredentialsLoader(ABC):
    @abstractmethod
    def load(self) -> ContainerRepositoryCredentials:
        raise NotImplementedError


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
