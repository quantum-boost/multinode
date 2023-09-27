from typing import Optional

from control_plane.types.datatypes import (
    VersionId,
    InvocationDefinition,
    InvocationInfo,
    InvocationsList,
    InvocationStatus,
    InvocationIdentifier,
)


class InvocationApiHandler:
    """
    API methods for starting function invocations, cancelling them, and getting their statuses
    and results.

    - Called by external code
    - Called by internal code that is making *nested* function invocations inside of
        an existing function invocation
    """

    def create_invocation(
        self,
        project_name: str,
        version_id: VersionId,
        function_name: str,
        invocation_definition: InvocationDefinition,
        time: int,
    ) -> InvocationInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises ParentInvocationDoesNotExist:
        """
        raise NotImplementedError

    def cancel_invocation(
        self,
        project_name: str,
        version_id: VersionId,
        function_name: str,
        invocation_id: str,
        time: int,
    ) -> InvocationInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        """
        raise NotImplementedError

    def delete_invocation(
        self,
        project_name: str,
        version_id: VersionId,
        function_name: str,
        invocation_id: str,
        time: int,
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises InvocationIsStillIncomplete:
        """
        raise NotImplementedError

    def get_invocation(
        self,
        project_name: str,
        version_id: VersionId,
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

    def list_invocations(
        self,
        project_name: str,
        version_id: VersionId,
        function_name: str,
        statuses: Optional[set[InvocationStatus]] = None,
        parent_invocation: Optional[InvocationIdentifier] = None,
        max_results: int = 50,
        cursor: Optional[str] = None,
    ) -> InvocationsList:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        """
        raise NotImplementedError
