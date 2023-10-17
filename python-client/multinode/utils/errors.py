class ProjectAlreadyExists(Exception):
    pass


class FunctionDoesNotExist(Exception):
    pass


class InvalidUseError(Exception):
    pass


class MissingEnvironmentVariableError(Exception):
    pass
