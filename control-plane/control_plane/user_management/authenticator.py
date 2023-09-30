from abc import ABC, abstractmethod
from typing import NamedTuple


class AuthResult(NamedTuple):
    user: str


class AbstractAuthenticator(ABC):
    @abstractmethod
    def authenticate(self, api_key: str) -> AuthResult:
        """
        :raises ApiKeyIsInvalid:
        """
        raise NotImplementedError
