from typing import Any

from control_plane.types.api_errors import ApiError, ErrorResponse


def document_possible_errors(
    possible_errors: list[type[ApiError]],
) -> dict[int | str, dict[str, Any]]:
    """
    Generate dictionary of status codes to response types / descriptions.
    Used solely for OpenAPI documentation, doesn't affect the way the code runs.

    :param possible_errors: a list of the possible errors that can be thrown
    :return: a dictionary of status codes to response types / descriptions, in the appropriate format for
       FastAPI decorators
    """
    responses: dict[int | str, dict[str, Any]] = dict()

    for error in possible_errors:
        if error.error_code() not in responses:
            responses[error.error_code()] = {
                "model": ErrorResponse,
                "description": error.error_message(),
            }
        else:
            responses[error.error_code()]["description"] += "; " + error.error_message()

    # NB we don't need to include the 200 successful response. FastAPI will automatically document this,
    # inferring the response type from the function return value.

    return responses
