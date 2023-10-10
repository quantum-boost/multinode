import logging
from typing import Optional

from control_plane.control.api.utils.version_reference_resolution import (
    resolve_version_reference,
)
from control_plane.data.data_store import DataStore
from control_plane.types.datatypes import (
    InvocationDefinition,
    InvocationInfo,
    InvocationsListForFunction,
    InvocationStatus,
    ParentInvocationDefinition,
)
from control_plane.types.random_ids import generate_random_id
from control_plane.types.version_reference import VersionReference


class InvocationApiHandler:
    """
    API methods for starting function invocations, cancelling them, and getting their statuses
    and results.

    - Called by external code
    - Called by internal code that is making *nested* function invocations inside of
        an existing function invocation
    """

    def __init__(self, data_store: DataStore) -> None:
        self._data_store = data_store

    def create_invocation(
        self,
        *,
        project_name: str,
        version_ref: VersionReference,
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
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

        invocation_id = generate_random_id("inv")

        self._data_store.invocations.create(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            parent_invocation=invocation_definition.parent_invocation,
            input=invocation_definition.input,
            cancellation_requested=False,
            invocation_status=InvocationStatus.RUNNING,
            creation_time=time,
            last_update_time=time,
        )

        logging.info(
            f"Created invocation ({project_name}, {version_id}, {function_name}, {invocation_id})"
            f" - status = {InvocationStatus.RUNNING}"
        )

        return self._data_store.invocations.get(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
        )

    def cancel_invocation(
        self,
        *,
        project_name: str,
        version_ref: VersionReference,
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
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

        self._data_store.invocations.update(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
            update_time=time,
            set_cancellation_requested=True,
        )

        logging.info(
            f"Updated invocation ({project_name}, {version_id}, {function_name}, {invocation_id})"
            f" - set cancellation request flag"
        )

        return self._data_store.invocations.get(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
        )

    def get_invocation(
        self,
        *,
        project_name: str,
        version_ref: VersionReference,
        function_name: str,
        invocation_id: str,
    ) -> InvocationInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises InvocationDoesNotExist:
        """
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

        return self._data_store.invocations.get(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            invocation_id=invocation_id,
        )

    def list_invocations(
        self,
        *,
        project_name: str,
        version_ref: VersionReference,
        function_name: str,
        max_results: Optional[int],
        initial_offset: Optional[str],
        status: Optional[InvocationStatus],
        parent_invocation: Optional[ParentInvocationDefinition],
    ) -> InvocationsListForFunction:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        :raises OffsetIsInvalid:
        """
        version_id = resolve_version_reference(
            project_name, version_ref, self._data_store
        )

        if max_results is None or max_results >= 50:
            sanitised_max_results = 50
        else:
            sanitised_max_results = max_results

        return self._data_store.invocations.list_for_function(
            project_name=project_name,
            version_id=version_id,
            function_name=function_name,
            max_results=sanitised_max_results,
            initial_offset=initial_offset,
            status=status,
            parent_invocation=parent_invocation,
        )
