from typing import NamedTuple, Optional

from control_plane.control.utils.random_ids import generate_random_id
from control_plane.data.data_store import DataStore
from control_plane.types.datatypes import InvocationInfo, InvocationStatus, WorkerStatus


class InvocationsLifecycleActions:
    """
    Control actions that progress Invocation objects through their lifecycle.
    These actions are executed periodically and in a single-threaded manner.
    """

    def __init__(self, data_store: DataStore):
        self._data_store = data_store

    def propagate_cancellation_requests_from_parents(self, time: int) -> None:
        """
        If an invocation:
          - is in RUNNING status
          - does not have cancellation_requested = True
          - has a parent with cancellation_requested = True
        then we should set cancellation_requested = True on this invocation.
        """
        # It's more efficient to iterate over the possible children, rather than iterating over the possible parents.
        # This is because the possible children have status = PENDING or RUNNING, so there shouldn't be too many
        # to load from the DB.
        running_invocations = self._data_store.invocations.list_all(
            statuses={InvocationStatus.RUNNING}
        )

        # Optimisation: iterate over invocations in the order in which they were created.
        # This usually reduces the number of iterations required to propagate cancellation requests to grandchildren.
        running_invocations = sorted(
            running_invocations, key=(lambda inv: inv.creation_time)
        )

        for invocation in running_invocations:
            if invocation.parent_invocation is not None:
                parent_invocation = self._data_store.invocations.get(
                    project_name=invocation.project_name,
                    version_id=invocation.version_id,
                    function_name=invocation.parent_invocation.function_name,
                    invocation_id=invocation.parent_invocation.invocation_id,
                )

                if parent_invocation.cancellation_requested:
                    self._data_store.invocations.update(
                        project_name=invocation.project_name,
                        version_id=invocation.version_id,
                        function_name=invocation.function_name,
                        invocation_id=invocation.invocation_id,
                        update_time=time,
                        set_cancellation_requested=True,
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
        => the invocation should be put in TERMINATED status

        Case 3:
          - none of the executions of the invocation have reached a SUCCEEDED or ABORTED outcome
          - max_retries limit has not yet been reached
              AND the cancellation_requested flag set to false
              AND the invocation has not timed out
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

        # To determine which pending invocations to run, we need to know about invocations that are currently running.
        # This is so that we can correctly apply resource restrictions.
        classification = classify_running_invocations(
            running_invocations=running_invocations,
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
                execution_end_time=None,
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


# Helper functions


class RunningInvocationsClassification(NamedTuple):
    invocations_to_terminate: list[InvocationInfo]
    invocations_to_create_executions_for: list[InvocationInfo]
    invocations_to_leave_untouched: list[InvocationInfo]


def classify_running_invocations(
    running_invocations: list[InvocationInfo], time: int
) -> RunningInvocationsClassification:
    raise NotImplementedError
