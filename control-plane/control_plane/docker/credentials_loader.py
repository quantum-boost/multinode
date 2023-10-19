from abc import ABC, abstractmethod

from control_plane.types.datatypes import ContainerRepositoryCredentials


class AbstractContainerRepositoryCredentialsLoader(ABC):
    @abstractmethod
    def load(self) -> ContainerRepositoryCredentials:
        raise NotImplementedError
