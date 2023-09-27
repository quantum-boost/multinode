from abc import ABC, abstractmethod


class ApiError(BaseException):
    @abstractmethod
    def error_message(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def error_code(self) -> int:
        raise NotImplementedError


class ExecutionDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "An execution with this ID does not exist for this invocation"

    def error_code(self) -> int:
        return 404


class ExecutionHasAlreadyBeenMarkedAsStarted(ApiError):
    def error_message(self) -> str:
        return "This execution has already been marked as started"

    def error_code(self) -> int:
        return 409


class ExecutionHasNotBeenMarkedAsStarted(ApiError):
    def error_message(self) -> str:
        return "This execution has not yet been marked as started"

    def error_code(self) -> int:
        return 409


class ExecutionHasAlreadyBeenFinalized(ApiError):
    def error_message(self) -> str:
        return "This execution has already been finalized"

    def error_code(self) -> int:
        return 409


class InvocationIsStillIncomplete(ApiError):
    def error_message(self) -> str:
        return "This invocation is not yet complete"

    def error_code(self) -> int:
        return 400


class ParentInvocationDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "The ID of the parent invocation is invalid"

    def error_code(self) -> int:
        return 400


class FunctionDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "A function with this name does not exist for this project version"

    def error_code(self) -> int:
        return 404


class VersionDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "No version exists with this ID exists for this project"

    def error_code(self) -> int:
        return 404


class VersionStillHasIncompleteInvocations(ApiError):
    def error_message(self) -> str:
        return (
            "This project version still has function invocations that are not complete"
        )

    def error_code(self) -> int:
        return 409


class ProjectAlreadyExists(ApiError):
    def error_message(self) -> str:
        return "A project with this ID already exists."

    def error_code(self) -> int:
        return 409


class ProjectDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "No project exists with this ID"

    def error_code(self) -> int:
        return 404


class ProjectStillHasActiveVersions(ApiError):
    def error_message(self) -> str:
        return "This project still has some active versions."

    def error_code(self) -> int:
        return 409


class InvocationDoesNotExist(ApiError):
    def error_message(self) -> str:
        return "An invocation with this ID does not exist for this function"

    def error_code(self) -> int:
        return 404
