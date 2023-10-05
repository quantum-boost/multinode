import logging

from control_plane.data.data_store import DataStore
from control_plane.types.datatypes import (
    ExecutionFinalResultPayload,
    ExecutionInfo,
    ExecutionTemporaryResultPayload,
)
from control_plane.types.version_reference import (
    VersionReference,
    resolve_version_reference,
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
        *,
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
        :raises InvocationDoesNotExist*:
        :raises ExecutionDoesNotExist:
        """
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

        return self._data_store.executions.get(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
        )

    def mark_execution_as_started(
        self,
        *,
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
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

        self._data_store.executions.update(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
            update_time=time,
            new_execution_start_time=time,
            should_already_have_started=False,
        )

        logging.info(
            f"Updated execution ({project_name}, {version_id}, {function_name}, {invocation_id}, {execution_id})"
            f" - started"
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
        *,
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
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

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

        logging.info(
            f"Updated execution ({project_name}, {version_id}, {function_name}, {invocation_id}, {execution_id})"
            f" - temporary result"
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
        *,
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
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

        self._data_store.executions.update(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
            update_time=time,
            new_execution_finish_time=time,
            new_outcome=final_result_payload.outcome,
            new_output=final_result_payload.final_output,
            new_error_message=final_result_payload.error_message,
            should_already_have_started=True,
            should_already_have_finished=False,
        )

        logging.info(
            f"Updated execution ({project_name}, {version_id}, {function_name}, {invocation_id}, {execution_id})"
            f" - finished"
        )

        return self._data_store.executions.get(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            execution_id=execution_id,
        )

    # Do not implement an API method for listing executions for a given invocation.
    # The .get_invocation method in InvocationsApiHandler already lists executions for a given invocation.
    # And besides, the ability to list executions for a given invocation belongs in InvocationsApiHandler
    # rather than ExecutionsApiHandler, since that method should be called by the invoker of the function,
    # not by the worker executing the function.
