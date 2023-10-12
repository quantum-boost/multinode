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
