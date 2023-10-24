from multinode.api_client.error_types import *  # noqa: F403


class InvalidUseError(Exception):
    pass


class MissingEnvironmentVariableError(Exception):
    pass


class InvocationFailedError(Exception):
    pass


class InvocationTimedOutError(Exception):
    pass


class InvocationCancelledError(Exception):
    pass


class FunctionInputSizeLimitExceeded(Exception):
    pass


class FunctionOutputSizeLimitExceeded(Exception):
    pass


class FunctionErrorMessageSizeLimitExceeded(Exception):
    pass


class ParameterValidationError(Exception):
    pass


__all__ = [  # noqa: F405
    "InvalidUseError",
    "MissingEnvironmentVariableError",
    "InvocationFailedError",
    "InvocationTimedOutError",
    "InvocationCancelledError",
    "MultinodeApiException",
    "ParameterValidationError",
    "FunctionInputSizeLimitExceeded",
    "FunctionOutputSizeLimitExceeded",
    "FunctionErrorMessageSizeLimitExceeded",
    *[
        error_class.__name__
        for error_class in MultinodeApiException.__subclasses__()  # noqa: F405
    ],
]
