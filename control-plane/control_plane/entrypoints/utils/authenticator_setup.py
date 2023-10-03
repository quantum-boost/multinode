from control_plane.entrypoints.utils.environment import get_mandatory_environment_variable
from control_plane.user_management.simple_authenticator import SimpleAuthenticator

CONTROL_PLANE_API_KEY_ENV = "CONTROL_PLANE_API_KEY"


def authenticator_from_environment_variables() -> SimpleAuthenticator:
    api_key = get_mandatory_environment_variable(CONTROL_PLANE_API_KEY_ENV)
    return SimpleAuthenticator(api_key)
