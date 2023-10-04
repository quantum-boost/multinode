from control_plane.types.errortypes import ApiKeyIsInvalid
from control_plane.user_management.authenticator import (
    AbstractAuthenticator,
    AuthResult,
)


class SimpleAuthenticator(AbstractAuthenticator):
    """An authenticator that recognises a single API key"""

    USER_NAME = "root"

    def __init__(self, api_key: str):
        self._api_key = api_key

    def authenticate(self, api_key: str) -> AuthResult:
        if api_key == self._api_key:
            return AuthResult(user=self.USER_NAME)
        else:
            raise ApiKeyIsInvalid
