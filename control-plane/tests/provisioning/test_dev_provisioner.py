from control_plane.provisioning.dev_provisioner import DevelopmentProvisioner, TOTAL_LOG_LINES
from control_plane.types.datatypes import ResourceSpec, WorkerStatus, WorkerDetails, WorkerType

PROJECT_NAME = "proj"
VERSION_ID = "ver"
FUNCTION_NAME = "func"
INVOCATION_ID = "inv"
EXECUTION_ID = "exe"
DOCKER_IMAGE = "image"
RESOURCE_SPEC = ResourceSpec(virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=4)


def test_lifecycle_with_natural_completion() -> None:
    lag_cycles = 10
    provisioner = DevelopmentProvisioner(lag_cycles=lag_cycles)

    prepared_function_details = provisioner.prepare_function(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        docker_image=DOCKER_IMAGE,
        resource_spec=RESOURCE_SPEC,
    )

    # Create worker. The worker should be created in RUNNING status
    worker_details = provisioner.provision_worker(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        resource_spec=RESOURCE_SPEC,
        prepared_function_details=prepared_function_details,
    )

    for _ in range(1000):
        assert provisioner.check_worker_status(worker_details=worker_details) == WorkerStatus.RUNNING

    # Notify of execution completion. After the lag has elapsed, the worker should go into TERMINATED
    provisioner.notify_of_execution_completion(worker_details=worker_details)

    for _ in range(lag_cycles):
        assert provisioner.check_worker_status(worker_details=worker_details) == WorkerStatus.RUNNING

    for _ in range(1000):
        assert provisioner.check_worker_status(worker_details=worker_details) == WorkerStatus.TERMINATED


def test_lifecycle_with_interruption_from_termination_signal() -> None:
    lag_cycles = 10
    provisioner = DevelopmentProvisioner(lag_cycles=lag_cycles)

    prepared_function_details = provisioner.prepare_function(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        docker_image=DOCKER_IMAGE,
        resource_spec=RESOURCE_SPEC,
    )

    # Create worker.
    worker_details = provisioner.provision_worker(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        resource_spec=RESOURCE_SPEC,
        prepared_function_details=prepared_function_details,
    )

    for _ in range(1000):
        assert provisioner.check_worker_status(worker_details=worker_details) == WorkerStatus.RUNNING

    # Interrupt by sending a termination signal
    provisioner.send_termination_signal_to_worker(worker_details=worker_details)

    # The worker should remain in RUNNING!
    for _ in range(1000):
        assert provisioner.check_worker_status(worker_details=worker_details) == WorkerStatus.RUNNING

    # Only once we've notified of completion does the worker go to TERMINATED
    provisioner.notify_of_execution_completion(worker_details=worker_details)

    for _ in range(lag_cycles):
        assert provisioner.check_worker_status(worker_details=worker_details) == WorkerStatus.RUNNING

    for _ in range(1000):
        assert provisioner.check_worker_status(worker_details=worker_details) == WorkerStatus.TERMINATED


WORKER_DETAILS = WorkerDetails(type=WorkerType.TEST, identifier="123", logs_identifier="mocked")


def test_get_logs_returned_in_one_page() -> None:
    provisioner = DevelopmentProvisioner(lag_cycles=1)
    logs_page = provisioner.get_worker_logs(worker_details=WORKER_DETAILS, max_lines=None, initial_offset=None)
    assert len(logs_page.log_lines) == TOTAL_LOG_LINES
    assert logs_page.log_lines[0] == "line-0"
    assert logs_page.log_lines[-1] == f"line-{TOTAL_LOG_LINES - 1}"
    assert logs_page.next_offset is None


def test_get_logs_split_across_two_pages() -> None:
    provisioner = DevelopmentProvisioner(lag_cycles=1)

    logs_page_1 = provisioner.get_worker_logs(worker_details=WORKER_DETAILS, max_lines=60, initial_offset=None)
    assert len(logs_page_1.log_lines) == 60
    assert logs_page_1.log_lines[0] == "line-0"
    assert logs_page_1.log_lines[-1] == "line-59"
    assert logs_page_1.next_offset is not None

    logs_page_2 = provisioner.get_worker_logs(
        worker_details=WORKER_DETAILS, max_lines=60, initial_offset=logs_page_1.next_offset
    )
    assert len(logs_page_2.log_lines) == 40
    assert logs_page_2.log_lines[0] == "line-60"
    assert logs_page_2.log_lines[-1] == "line-99"
    assert logs_page_2.next_offset is None
