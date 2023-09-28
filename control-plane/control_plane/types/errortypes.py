from abc import ABC, abstractmethod


class ApiError(BaseException):
    @abstractmethod
    def error_message(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def error_code(self) -> int:
        raise NotImplementedError


class ExecutionAlreadyExists(ApiError):
    def error_message(self) -> str:
        return "An execution with this ID already exists for this invocation"

    def error_code(self) -> int:
        return 409


class ExecutionDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "An execution with this ID does not exist for this invocation"

    def error_code(self) -> int:
        return 404


class ExecutionHasAlreadyStarted(ApiError):
    def error_message(self) -> str:
        return "This execution has already started"

    def error_code(self) -> int:
        return 409


class ExecutionHasNotStarted(ApiError):
    def error_message(self) -> str:
        return "This execution has not yet started"

    def error_code(self) -> int:
        return 409


class ExecutionHasAlreadyFinished(ApiError):
    def error_message(self) -> str:
        return "This execution has already finished"

    def error_code(self) -> int:
        return 409


class ExecutionHasNotFinished(ApiError):
    def error_message(self) -> str:
        return "This execution has not yet finished"

    def error_code(self) -> int:
        return 409


class InvocationAlreadyExists(ApiError):
    def error_message(self) -> str:
        return "An invocation with this ID already exists for this function"

    def error_code(self) -> int:
        return 409


class InvocationDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "An invocation with this ID does not exist for this function"

    def error_code(self) -> int:
        return 404


class ParentInvocationDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "The ID of the parent invocation is invalid"

    def error_code(self) -> int:
        return 400


class FunctionAlreadyExists(ApiError):
    def error_message(self) -> str:
        return "A function with this name already exists for this project version"

    def error_code(self) -> int:
        return 409


class FunctionDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "A function with this name does not exist for this project version"

    def error_code(self) -> int:
        return 404


class VersionAlreadyExists(ApiError):
    def error_message(self) -> str:
        return "A version with this ID already exists for this project"

    def error_code(self) -> int:
        return 409


class VersionDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "A version with this ID does not exist for this project"

    def error_code(self) -> int:
        return 404


class ProjectAlreadyExists(ApiError):
    def error_message(self) -> str:
        return "A project with this ID already exists."

    def error_code(self) -> int:
        return 409


class ProjectDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "A project with this ID does not exist"

    def error_code(self) -> int:
        return 404
