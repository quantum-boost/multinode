from abc import ABC, abstractmethod

from pydantic import BaseModel


class ErrorResponse(BaseModel):
    detail: str


class ApiError(Exception, ABC):
    def response(self) -> ErrorResponse:
        return ErrorResponse(detail=self.error_message())

    @staticmethod
    @abstractmethod
    def error_message() -> str:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def error_code() -> int:
        raise NotImplementedError


# Errors about request headers


class ApiKeyIsInvalid(ApiError):
    @staticmethod
    def error_message() -> str:
        return "The API key is invalid"

    @staticmethod
    def error_code() -> int:
        return 403


# Errors about query params


class OffsetIsInvalid(ApiError):
    @staticmethod
    def error_message() -> str:
        return "The next offset is in an invalid format"

    @staticmethod
    def error_code() -> int:
        return 400


class ParentFunctionNameIsMissing(ApiError):
    @staticmethod
    def error_message() -> str:
        return "The parent function name is missing"

    @staticmethod
    def error_code() -> int:
        return 400


# Errors about path parameters


class ProjectNameIsTooLong(ApiError):
    @staticmethod
    def error_message() -> str:
        return "The project name is too long"

    @staticmethod
    def error_code() -> int:
        return 400


# Errors thrown in API code


class ProjectIsBeingDeleted(ApiError):
    @staticmethod
    def error_message() -> str:
        return "The project is being deleted"

    @staticmethod
    def error_code() -> int:
        return 400


# Errors from the database


class ParentInvocationIdIsMissing(ApiError):
    @staticmethod
    def error_message() -> str:
        return "The parent invocation ID is missing"

    @staticmethod
    def error_code() -> int:
        return 400


class ExecutionAlreadyExists(ApiError):
    @staticmethod
    def error_message() -> str:
        return "An execution with this ID already exists for this invocation"

    @staticmethod
    def error_code() -> int:
        return 409


class ExecutionDoesNotExist(ApiError):
    @staticmethod
    def error_message() -> str:
        return "An execution with this ID does not exist for this invocation"

    @staticmethod
    def error_code() -> int:
        return 404


class ExecutionHasAlreadyStarted(ApiError):
    @staticmethod
    def error_message() -> str:
        return "This execution has already started"

    @staticmethod
    def error_code() -> int:
        return 409


class ExecutionHasNotStarted(ApiError):
    @staticmethod
    def error_message() -> str:
        return "This execution has not yet started"

    @staticmethod
    def error_code() -> int:
        return 409


class ExecutionHasAlreadyFinished(ApiError):
    @staticmethod
    def error_message() -> str:
        return "This execution has already finished"

    @staticmethod
    def error_code() -> int:
        return 409


class ExecutionHasNotFinished(ApiError):
    @staticmethod
    def error_message() -> str:
        return "This execution has not yet finished"

    @staticmethod
    def error_code() -> int:
        return 409


class InvocationAlreadyExists(ApiError):
    @staticmethod
    def error_message() -> str:
        return "An invocation with this ID already exists for this function"

    @staticmethod
    def error_code() -> int:
        return 409


class InvocationDoesNotExist(ApiError):
    @staticmethod
    def error_message() -> str:
        return "An invocation with this ID does not exist for this function"

    @staticmethod
    def error_code() -> int:
        return 404


class ParentInvocationDoesNotExist(ApiError):
    @staticmethod
    def error_message() -> str:
        return "The ID of the parent invocation is invalid"

    @staticmethod
    def error_code() -> int:
        return 400


class FunctionAlreadyExists(ApiError):
    @staticmethod
    def error_message() -> str:
        return "A function with this name already exists for this project version"

    @staticmethod
    def error_code() -> int:
        return 409


class FunctionDoesNotExist(ApiError):
    @staticmethod
    def error_message() -> str:
        return "A function with this name does not exist for this project version"

    @staticmethod
    def error_code() -> int:
        return 404


class VersionAlreadyExists(ApiError):
    @staticmethod
    def error_message() -> str:
        return "A version with this ID already exists for this project"

    @staticmethod
    def error_code() -> int:
        return 409


class VersionDoesNotExist(ApiError):
    @staticmethod
    def error_message() -> str:
        return "A version with this ID does not exist for this project"

    @staticmethod
    def error_code() -> int:
        return 404


class ProjectAlreadyExists(ApiError):
    @staticmethod
    def error_message() -> str:
        return "A project with this ID already exists."

    @staticmethod
    def error_code() -> int:
        return 409


class ProjectDoesNotExist(ApiError):
    @staticmethod
    def error_message() -> str:
        return "A project with this ID does not exist"

    @staticmethod
    def error_code() -> int:
        return 404
