from typing import Optional

from control_plane.provisioning.provisioner import (
    AbstractProvisioner,
    LogsResult,
    UnrecoverableProvisioningError,
)
from control_plane.types.datatypes import (
    PreparedFunctionDetails,
    ResourceSpec,
    WorkerDetails,
    WorkerStatus,
    WorkerType,
)

DUMMY_PROVISIONING_ERROR_MSG = "provisioning error"


class DummyProvisioner(AbstractProvisioner):
    """
    An implementation of AbstractProvisioner that mocks the behaviour of a real provision.
    No actual workers are created. Used in tests.
    """

    def __init__(self, should_fail_provisioning: bool = False) -> None:
        # Each set contains *identifiers* of workers.
        self._workers_provisioned: set[str] = set()
        self._workers_sent_termination_signal: set[str] = set()
        self._workers_terminated: set[str] = set()
        self._should_fail_provisioning = should_fail_provisioning

    def prepare_function(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        docker_image: str,
        resource_spec: ResourceSpec,
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
        if self._should_fail_provisioning:
            raise UnrecoverableProvisioningError(DUMMY_PROVISIONING_ERROR_MSG)

        identifier = create_identifier(
            project_name, version_id, function_name, invocation_id, execution_id
        )
        self._workers_provisioned.add(identifier)

        return WorkerDetails(
            type=WorkerType.TEST, identifier=identifier, logs_identifier="mocked"
        )

    def send_termination_signal_to_worker(
        self, *, worker_details: WorkerDetails
    ) -> None:
        if worker_details.identifier in self._workers_provisioned:
            self._workers_sent_termination_signal.add(worker_details.identifier)

    def check_worker_status(self, *, worker_details: WorkerDetails) -> WorkerStatus:
        if (
            worker_details.identifier in self._workers_provisioned
            and worker_details.identifier not in self._workers_terminated
        ):
            return WorkerStatus.RUNNING
        else:
            return WorkerStatus.TERMINATED

    def get_worker_logs(
        self,
        *,
        worker_details: WorkerDetails,
        max_lines: Optional[int],
        initial_offset: Optional[str],
    ) -> LogsResult:
        return LogsResult(log_lines=["hello", "world"], next_offset=None)

    # The following methods are not part of the AbstractProvisioner interface.
    # They are used in tests to mock the behaviour of the provisioner
    # or to assert something about what state the worker is in.

    def mock_worker_termination(self, worker_details: WorkerDetails) -> None:
        self._workers_terminated.add(worker_details.identifier)

    def worker_is_provisioned(self, worker_details: WorkerDetails) -> bool:
        return worker_details.identifier in self._workers_provisioned

    def worker_has_received_termination_signal(
        self, worker_details: WorkerDetails
    ) -> bool:
        return worker_details.identifier in self._workers_sent_termination_signal

    def worker_is_terminated(self, worker_details: WorkerDetails) -> bool:
        return worker_details.identifier in self._workers_terminated


def create_identifier(
    project_name: str,
    version_id: str,
    function_name: str,
    invocation_id: str,
    execution_id: str,
) -> str:
    return f"{project_name}/{version_id}/{function_name}/{invocation_id}/{execution_id}"
