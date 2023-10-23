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


__all__ = [  # noqa: F405
    "InvalidUseError",
    "MissingEnvironmentVariableError",
    "InvocationFailedError",
    "InvocationTimedOutError",
    "InvocationCancelledError",
    "MultinodeApiException",
    *[
        error_class.__name__
        for error_class in MultinodeApiException.__subclasses__()  # noqa: F405
    ],
]
