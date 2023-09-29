from control_plane.control.utils.version_reference_utils import (
    resolve_version_reference,
)
from control_plane.data.data_store import DataStore
from control_plane.types.datatypes import (
    VersionReference,
    ExecutionInfo,
    ExecutionTemporaryResultPayload,
    ExecutionFinalResultPayload,
    ExecutionsListForInvocation,
)


class ExecutionApiHandler:
    """
    API methods for getting the description of a function execution, for supplying progress updates
    and for supplying the final result.

    Called by a worker.
    """

    def __init__(self, data_store: DataStore):
        self._data_store = data_store

    def get_execution(
        self,
        project_name: str,
        version_ref: VersionReference,
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
        version_id = resolve_version_reference(project_name, version_ref, self._data_store)

        return self._data_store.executions.get(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
        )

    def mark_execution_as_started(
        self,
        project_name: str,
        version_ref: VersionReference,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        time: int,
    ) -> ExecutionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        :raises ExecutionHasAlreadyStarted:
        """
        version_id = resolve_version_reference(project_name, version_ref, self._data_store)

        self._data_store.executions.update(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
            update_time=time,
            execution_start_time=time,
            should_already_have_started=False,
        )

        return self._data_store.executions.get(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
        )

    def upload_temporary_execution_result(
        self,
        project_name: str,
        version_ref: VersionReference,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        temporary_result_payload: ExecutionTemporaryResultPayload,
        time: int,
    ) -> ExecutionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        :raises ExecutionHasNotStarted:
        :raises ExecutionHasAlreadyFinished:
        """
        version_id = resolve_version_reference(project_name, version_ref, self._data_store)

        self._data_store.executions.update(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
            update_time=time,
            new_output=temporary_result_payload.latest_output,
            should_already_have_started=True,
            should_already_have_finished=False,
        )

        return self._data_store.executions.get(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
        )

    def set_final_execution_result(
        self,
        project_name: str,
        version_ref: VersionReference,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        final_result_payload: ExecutionFinalResultPayload,
        time: int,
    ) -> ExecutionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        :raises ExecutionDoesNotExist:
        :raises ExecutionHasNotStarted:
        :raises ExecutionHasAlreadyFinished:
        """
        version_id = resolve_version_reference(project_name, version_ref, self._data_store)

        self._data_store.executions.update(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
            update_time=time,
            execution_end_time=time,
            new_outcome=final_result_payload.outcome,
            new_output=final_result_payload.final_output,
            new_error_message=final_result_payload.error_message,
            should_already_have_started=True,
            should_already_have_finished=False,
        )

        return self._data_store.executions.get(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
        )

    def list_executions(
        self,
        project_name: str,
        version_ref: VersionReference,
        function_name: str,
        invocation_id: str,
    ) -> ExecutionsListForInvocation:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        """
        version_id = resolve_version_reference(project_name, version_ref, self._data_store)

        return self._data_store.executions.list_for_invocation(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
        )
