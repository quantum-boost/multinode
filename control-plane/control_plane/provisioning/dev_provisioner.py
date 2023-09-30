from typing import Union, Optional

from control_plane.provisioning.provisioner import AbstractProvisioner, LogsResult
from control_plane.types.datatypes import PreparedFunctionDetails, ResourceSpec, WorkerType, WorkerDetails, WorkerStatus


TOTAL_LOG_LINES = 100


class DevelopmentProvisioner(AbstractProvisioner):
    """
    A mocked-up provisioner that is used primarily by developers when building clients.
    Doesn't create any real resources.
    """

    def __init__(self, lag_cycles: int) -> None:
        """
        :param lag_cycles: Number of cycles between an execution finishing and the worker terminating.
                           (A "cycle" = one call to .check_worker_status)
        """
        # Dictionary structure: {worker_identifier: num_remaining_cycles}
        # Execution not finished => num_remaining_cycles = infinity
        # Execution finished => num_remaining_cycles ticks counts from lag_cycles to 0
        self._lag_cycles = lag_cycles
        self._workers_to_remaining_cycles: dict[str, Union[int, float]] = dict()

    def prepare_function(
        self, *, project_name: str, version_id: str, function_name: str, docker_image: str, resource_spec: ResourceSpec
    ) -> PreparedFunctionDetails:
        return PreparedFunctionDetails(type=WorkerType.TEST, identifier="mocked")

    def provision_worker(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        invocation_id: str,
        execution_id: str,
        resource_spec: ResourceSpec,
        prepared_function_details: PreparedFunctionDetails,
    ) -> WorkerDetails:
        identifier = create_identifier(project_name, version_id, function_name, invocation_id, execution_id)
        self._workers_to_remaining_cycles[identifier] = float("inf")

        return WorkerDetails(type=WorkerType.TEST, identifier=identifier, logs_identifier="mocked")

    def send_termination_signal_to_worker(self, *, worker_details: WorkerDetails) -> None:
        # Do nothing. For dev purposes, the nicest developer experience is if the worker stays RUNNING until
        # the worker client submits the final result for the execution.
        pass

    def check_worker_status(self, *, worker_details: WorkerDetails) -> WorkerStatus:
        if worker_details.identifier in self._workers_to_remaining_cycles:
            self._workers_to_remaining_cycles[worker_details.identifier] -= 1

            if self._workers_to_remaining_cycles[worker_details.identifier] < 0:
                del self._workers_to_remaining_cycles[worker_details.identifier]

        if worker_details.identifier in self._workers_to_remaining_cycles:
            return WorkerStatus.RUNNING
        else:
            return WorkerStatus.TERMINATED

    def notify_of_execution_completion(self, *, worker_details: WorkerDetails) -> None:
        if worker_details.identifier in self._workers_to_remaining_cycles:
            remaining_cycles_before_update = self._workers_to_remaining_cycles[worker_details.identifier]
            remaining_cycles_after_update = min(remaining_cycles_before_update, self._lag_cycles)
            self._workers_to_remaining_cycles[worker_details.identifier] = remaining_cycles_after_update

    def get_worker_logs(
        self, *, worker_details: WorkerDetails, max_lines: Optional[int], initial_offset: Optional[str]
    ) -> LogsResult:
        if initial_offset is not None:
            left_bound = int(initial_offset)  # inclusive bound
        else:
            left_bound = 0

        if max_lines is not None:
            right_bound = min(TOTAL_LOG_LINES, left_bound + max_lines)  # exclusive bound
        else:
            right_bound = TOTAL_LOG_LINES

        log_lines = [f"line-{i}" for i in range(left_bound, right_bound)]

        if right_bound < TOTAL_LOG_LINES:
            next_offset = str(right_bound)
        else:
            next_offset = None

        return LogsResult(log_lines=log_lines, next_offset=next_offset)


def create_identifier(
    project_name: str, version_id: str, function_name: str, invocation_id: str, execution_id: str
) -> str:
    return f"{project_name}/{version_id}/{function_name}/{invocation_id}/{execution_id}"
