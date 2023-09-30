import pytest

from control_plane.types.errortypes import ApiKeyIsInvalid
from control_plane.user_management.simple_authenticator import SimpleAuthenticator


def test_get_current_user_with_valid_api_key() -> None:
    correct_api_key = "butterflyburger"
    authenticator = SimpleAuthenticator(api_key=correct_api_key)
    auth_result = authenticator.authenticate(correct_api_key)
    assert auth_result.user == "root"


def test_get_current_user_with_invalid_api_key() -> None:
    correct_api_key = "butterflyburger"
    authenticator = SimpleAuthenticator(api_key=correct_api_key)

    incorrect_api_key = "finopitta"
    with pytest.raises(ApiKeyIsInvalid):
        authenticator.authenticate(incorrect_api_key)
