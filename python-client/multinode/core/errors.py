class ProjectAlreadyExists(Exception):
    pass


class FunctionDoesNotExist(Exception):
    def __init__(self, project_name: str, version_id: str, function_name: str):
        super().__init__(
            f'Function "{function_name}" does not exist '
            f'on version "{version_id}" of project "{project_name}".'
        )


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
