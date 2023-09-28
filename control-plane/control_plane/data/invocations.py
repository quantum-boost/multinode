from typing import Optional

from control_plane.types.datatypes import (
    InvocationStatus,
    InvocationInfo,
    InvocationIdentifier,
    InvocationsListForFunction,
)


class InvocationsTable:
    def create(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        parent_invocation_function_name: Optional[str],
        parent_invocation_invocation_id: Optional[str],
        input: str,
        cancellation_requested: bool,
        invocation_status: InvocationStatus,
        creation_time: int,
        last_update_time: int
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationAlreadyExists:
        :raises ParentInvocationDoesNotExist:
        """
        raise NotImplementedError

    def update(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        update_time: int,
        set_cancellation_requested: bool = False,
        new_invocation_status: Optional[InvocationStatus] = None
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        """
        raise NotImplementedError

    def get(
        self,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
    ) -> InvocationInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        """
        raise NotImplementedError

    def list_for_function(
        self,
        project_name: str,
        version_id: str,
        function_name: str,
        max_results: int,
        cursor: Optional[str] = None,
        statuses: Optional[set[InvocationStatus]] = None,
        parent_invocation: Optional[InvocationIdentifier] = None,
    ) -> InvocationsListForFunction:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        """
        raise NotImplementedError

    def list_all(self, *, statuses: set[InvocationStatus]) -> list[InvocationInfo]:
        """
        The statuses argument must be populated. It is unwise to call this method with the TERMINATED status,
        since that is likely to return a very large number of results.
        """
        raise NotImplementedError
