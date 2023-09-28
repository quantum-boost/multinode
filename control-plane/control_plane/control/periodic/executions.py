from typing import NamedTuple

from control_plane.data.data_store import DataStore
from control_plane.provisioning.provisioner import AbstractProvisioner
from control_plane.types.datatypes import ExecutionInfo, WorkerStatus


class ExecutionsLifecycleActions:
    """
    Control actions that progress Execution objects through their lifecycle.
    These actions are executed periodically and in a single-threaded manner.
    """

    def __init__(self, data_store: DataStore, provisioner: AbstractProvisioner):
        self._data_store = data_store
        self._provisioner = provisioner

    def handle_pending_executions(self, time: int) -> None:
        """
        A worker should be provisioned, and the worker status should be set to RUNNING.
        """
        pending_executions = self._data_store.executions.list_all(
            worker_statuses={WorkerStatus.PENDING}
        )

        for execution in pending_executions:
            assert execution.prepared_function_details is not None

            self._data_store.executions.update(
                project_name=execution.project_name,
                version_id=execution.version_id,
                function_name=execution.function_name,
                invocation_id=execution.invocation_id,
                execution_id=execution.execution_id,
                update_time=time,
                new_worker_status=WorkerStatus.PROVISIONING,
            )

            worker_details = self._provisioner.provision_worker(
                project_name=execution.project_name,
                version_id=execution.version_id,
                function_name=execution.function_name,
                invocation_id=execution.invocation_id,
                execution_id=execution.execution_id,
                resource_spec=execution.resource_spec,
                prepared_function_details=execution.prepared_function_details,
            )

            self._data_store.executions.update(
                project_name=execution.project_name,
                version_id=execution.version_id,
                function_name=execution.function_name,
                invocation_id=execution.invocation_id,
                execution_id=execution.execution_id,
                update_time=time,
                new_worker_status=WorkerStatus.RUNNING,
                new_worker_details=worker_details,
            )

    def handle_running_executions(self, time: int) -> None:
        """
        The worker may have died (e.g. due to execution finishing naturally, due to a sigterm signal or due to
        a hardware failure). If so, then the worker status should be set to TERMINATED.
        """
        running_executions = self._data_store.executions.list_all(
            worker_statuses={WorkerStatus.RUNNING}
        )

        for execution in running_executions:
            assert execution.worker_details is not None

            worker_status = self._provisioner.check_worker_status(
                worker_details=execution.worker_details
            )

            if worker_status == WorkerStatus.TERMINATED:
                self._data_store.executions.update(
                    project_name=execution.project_name,
                    version_id=execution.version_id,
                    function_name=execution.function_name,
                    invocation_id=execution.invocation_id,
                    execution_id=execution.execution_id,
                    update_time=time,
                    new_worker_status=WorkerStatus.TERMINATED,
                )

    def handle_running_executions_requiring_termination_signal(self, time: int) -> None:
        """
        Case:
          - associated with an invocation which has cancellation_requested = True OR has timed out
          - doesn't yet have termination_signal_sent = True
        => we should send a termination signal to the worker to abort the execution,
           and set termination_signal_sent = True
        """
        running_executions = self._data_store.executions.list_all(
            worker_statuses={WorkerStatus.RUNNING}
        )

        classification = classify_running_executions(running_executions, time)

        for execution in classification.executions_requiring_termination_signal:
            assert execution.worker_details is not None

            self._provisioner.send_termination_signal_to_worker(
                worker_details=execution.worker_details
            )

            self._data_store.executions.update(
                project_name=execution.project_name,
                version_id=execution.version_id,
                function_name=execution.function_name,
                invocation_id=execution.invocation_id,
                execution_id=execution.execution_id,
                update_time=time,
                set_termination_signal_sent=True,
            )

    def handle_executions_stuck_in_provisioning(self, time: int) -> None:
        """
        This handles the extremely rare edge case where handle_pending_executions is interrupted in between
        provisioning a worker and saving the worker status as RUNNING.

        In this situation, we should find the worker (e.g. using tags), and then terminate the worker.

        It's important to do this, because the user's code may suffer from race conditions if two workers are
        running simultaneously.
        """
        executions_stuck_in_provisioning = self._data_store.executions.list_all(
            worker_statuses={WorkerStatus.PROVISIONING}
        )

        # TODO: Identity the worker (e.g. using tags), and kill the worker.

        for execution in executions_stuck_in_provisioning:
            self._data_store.executions.update(
                project_name=execution.project_name,
                version_id=execution.version_id,
                function_name=execution.function_name,
                invocation_id=execution.invocation_id,
                execution_id=execution.execution_id,
                update_time=time,
                new_worker_status=WorkerStatus.TERMINATED,
            )


# Helper functions


class RunningExecutionsClassification(NamedTuple):
    executions_requiring_termination_signal: list[ExecutionInfo]
    executions_to_leave_alone: list[ExecutionInfo]


def classify_running_executions(
    running_executions: list[ExecutionInfo], time: int
) -> RunningExecutionsClassification:
    raise NotImplementedError
