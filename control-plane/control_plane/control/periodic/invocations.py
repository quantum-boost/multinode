import logging

from control_plane.control.periodic.invocations_cancellation_requests_helper import (
    classify_invocations_for_cancellation_requests,
)
from control_plane.control.periodic.invocations_helper import (
    classify_running_invocations,
)
from control_plane.data.data_store import DataStore
from control_plane.types.datatypes import FunctionStatus, InvocationStatus, WorkerStatus
from control_plane.types.random_ids import generate_random_id


class InvocationsLifecycleActions:
    """
    Control actions that progress Invocation objects through their lifecycle.
    These actions are executed periodically and in a single-threaded manner.
    """

    def __init__(self, data_store: DataStore):
        self._data_store = data_store

    def run_all(self, time: int) -> None:
        # Any ordering will work. However, this choice of ordering should slightly improve user experience,
        # since it reduces the chance of an execution being created for a child invocation
        # when its parent has already been cancelled.

        self.handle_running_invocations_requiring_cancellation_requests(time)
        self.handle_running_invocations(time)

    def handle_running_invocations_requiring_cancellation_requests(
        self, time: int
    ) -> None:
        """
        If an invocation:
          - is in RUNNING status
          - does not have cancellation_requested = True
          - has a parent with cancellation_requested = True
        then we should set cancellation_requested = True on this invocation.

        Also, if an invocation:
          - is in RUNNING status
          - does not have cancellation_requested = True
          - belongs to a project with deletion_requested = True
        then we should set cancellation_requested = True on this invocation
        """
        # It's more efficient to iterate over the possible children, rather than iterating over the possible parents.
        # This is because the possible children have status = RUNNING, so there shouldn't be too many
        # to load from the DB.
        running_invocations = self._data_store.invocations.list_all(
            statuses={InvocationStatus.RUNNING}
        )

        projects = self._data_store.projects.list().projects

        classification = classify_invocations_for_cancellation_requests(
            running_invocations, projects
        )

        for invocation in classification.invocations_to_set_cancellation_requested:
            self._data_store.invocations.update(
                project_name=invocation.project_name,
                version_id=invocation.version_id,
                function_name=invocation.function_name,
                invocation_id=invocation.invocation_id,
                update_time=time,
                set_cancellation_requested=True,
            )

            logging.info(
                f"Updated invocation ({invocation.project_name}, {invocation.version_id}, "
                f"{invocation.function_name}, {invocation.invocation_id})"
                f" - set cancellation request flag"
            )

    def handle_running_invocations(self, time: int) -> None:
        """
        If an invocation:
          - is in RUNNING status
          - has no executions associated with it
             OR all executions associated with it are already TERMINATED
        then we need to do something to move this invocation forward in its lifecycle.

        Case 1:
          - at least one execution of the invocation has reached a SUCCEEDED or ABORTED outcome
        => the invocation should be put in TERMINATED status

        Case 2:
          - none of the executions of the invocation have reached a SUCCEEDED or ABORTED outcome
          - the max_retries limit has been reached
              OR the cancellation_requested flag is set to true
              OR the invocation has timed out
              OR the project is in the process of being deleted
        => the invocation should be put in TERMINATED status

        Case 3:
          - none of the executions of the invocation have reached a SUCCEEDED or ABORTED outcome
          - max_retries limit has not yet been reached
              AND the cancellation_requested flag set to false
              AND the invocation has not timed out
              AND the project is not in the process of being deleted
          - the function is in READY status
              AND creating a new execution for this function will not exceed the function's resource limits
        => the invocation should remain in RUNNING status
           AND a new execution should be created for this invocation

        Default case:
        => the invocation should remain in RUNNING status, and nothing should be done
        """
        running_invocations = self._data_store.invocations.list_all(
            statuses={InvocationStatus.RUNNING}
        )

        functions_in_ready_status = self._data_store.functions.list_all(
            statuses={FunctionStatus.READY}
        )

        classification = classify_running_invocations(
            running_invocations=running_invocations,
            functions_in_ready_status=functions_in_ready_status,
            time=time,
        )

        for invocation in classification.invocations_to_create_executions_for:
            self._data_store.invocations.update(
                project_name=invocation.project_name,
                version_id=invocation.version_id,
                function_name=invocation.function_name,
                invocation_id=invocation.invocation_id,
                update_time=time,  # the only update is that the last_update_time field is getting bumped
            )

            execution_id = generate_random_id("exe")

            self._data_store.executions.create(
                project_name=invocation.project_name,
                version_id=invocation.version_id,
                function_name=invocation.function_name,
                invocation_id=invocation.invocation_id,
                execution_id=execution_id,
                worker_status=WorkerStatus.PENDING,
                worker_details=None,
                termination_signal_sent=False,
                outcome=None,
                output=None,
                error_message=None,
                creation_time=time,
                last_update_time=time,
                execution_start_time=None,
                execution_finish_time=None,
            )

            logging.info(
                f"Created execution ({invocation.project_name}, {invocation.version_id}, "
                f"{invocation.function_name}, {invocation.invocation_id}, {execution_id})"
                f" - worker status = {WorkerStatus.PENDING}"
            )

        for invocation in classification.invocations_to_terminate:
            self._data_store.invocations.update(
                project_name=invocation.project_name,
                version_id=invocation.version_id,
                function_name=invocation.function_name,
                invocation_id=invocation.invocation_id,
                update_time=time,
                new_invocation_status=InvocationStatus.TERMINATED,
            )

            logging.info(
                f"Updated invocation ({invocation.project_name}, {invocation.version_id}, "
                f"{invocation.function_name}, {invocation.invocation_id})"
                f" - status = {InvocationStatus.TERMINATED}"
            )
