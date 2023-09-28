from typing import Optional

from control_plane.types.datatypes import (
    FunctionInfo,
    FunctionsListForVersion,
    FunctionStatus,
    PreparedFunctionDetails,
    ResourceSpec,
    ExecutionSpec,
)


class FunctionsTable:
    def create(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        docker_image: str,
        resource_spec: ResourceSpec,
        execution_spec: ExecutionSpec,
        function_status: FunctionStatus,
        prepared_function_details: Optional[PreparedFunctionDetails]
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionAlreadyExists:
        """
        raise NotImplementedError

    def update(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        new_status: Optional[FunctionStatus],
        new_prepared_function_details: Optional[PreparedFunctionDetails]
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        """
        raise NotImplementedError

    def get(
        self, *, project_name: str, version_id: str, function_name: str
    ) -> FunctionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        """
        raise NotImplementedError

    def list_for_project_version(
        self, *, project_name: str, version_id: str
    ) -> FunctionsListForVersion:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        """
        raise NotImplementedError

    def list_all(self, *, statuses: set[FunctionStatus]) -> list[FunctionInfo]:
        raise NotImplementedError
