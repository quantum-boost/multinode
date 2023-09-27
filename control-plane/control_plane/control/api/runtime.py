from control_plane.types.datatypes import (
    VersionId,
    ExecutionInfo,
    ExecutionTemporaryResultPayload,
    ExecutionFinalResultPayload,
)


class RuntimeApiHandler:
    """
    API methods for getting the description of a function execution, for supplying progress updates
    and for supplying the final result.

    Called by a worker.
    """

    def get_execution(
        self,
        project_name: str,
        version_id: VersionId,
        function_name: str,
        invocation_id: str,
        execution_id: str,
    ) -> ExecutionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        """
        raise NotImplementedError

    def mark_execution_as_started(
        self,
        project_name: str,
        version_id: VersionId,
        function_name: str,
        invocation_id: str,
        execution_id: str,
    ) -> ExecutionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        :raises ExecutionHasAlreadyBeenMarkedAsStarted:
        """
        raise NotImplementedError

    def upload_temporary_execution_result(
        self,
        project_name: str,
        version_id: VersionId,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        temporary_result_payload: ExecutionTemporaryResultPayload,
    ) -> ExecutionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        :raises ExecutionHasNotBeenMarkedAsStarted:
        :raises ExecutionHasAlreadyBeenFinalized:
        """
        raise NotImplementedError

    def set_final_execution_result(
        self,
        project_name: str,
        version_id: VersionId,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        final_result_payload: ExecutionFinalResultPayload,
    ) -> ExecutionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        :raises ExecutionHasNotBeenMarkedAsStarted:
        :raises ExecutionHasAlreadyBeenFinalized:
        """
        raise NotImplementedError
