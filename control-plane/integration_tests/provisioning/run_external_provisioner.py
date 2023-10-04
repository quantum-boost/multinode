import time
from typing import Optional

from control_plane.provisioning.external_provisioner import ExternalProvisioner
from control_plane.types.datatypes import (
    ResourceSpec,
    WorkerStatus,
    WorkerDetails,
    WorkerType,
)

PROJECT_NAME = "project"
VERSION_ID = "version"
FUNCTION_NAME = "function"
INVOCATION_ID = "invocation"
EXECUTION_ID = "execution"

# Some settings that you may wish to reconfigure
DOCKER_IMAGE = "nginx:latest"
RESOURCE_SPEC = ResourceSpec(virtual_cpus=0.25, memory_gbs=0.5, max_concurrency=1)
NUM_LOG_LINES_PER_PAGE = 3
MIN_LOG_PAGES_WITH_AT_LEAST_ONE_LINE = 2

# Change to match your external provisioner
PROVISIONER_API_URL = (
    "https://ro2p3s7gg6c5l5fwu44iwqvd6i0zypiw.lambda-url.eu-west-2.on.aws/"
)
PROVISIONER_API_KEY = "lemonandherb"

# Be careful with interrupting this!!! You may leave tasks running in our AWS account, which will cost money.


def main() -> None:
    provisioner = ExternalProvisioner(
        provisioner_api_url=PROVISIONER_API_URL, provisioner_api_key=PROVISIONER_API_KEY
    )

    prepared_function_details = provisioner.prepare_function(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        docker_image=DOCKER_IMAGE,
        resource_spec=RESOURCE_SPEC,
    )

    print("Prepared function details:", prepared_function_details)

    worker_details = provisioner.provision_worker(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        resource_spec=RESOURCE_SPEC,
        prepared_function_details=prepared_function_details,
    )

    print("Worker details:", worker_details)

    status = provisioner.check_worker_status(worker_details=worker_details)
    print("Status:", status)

    assert status == WorkerStatus.RUNNING

    current_logs_offset: Optional[str] = None

    num_logs_pages_containing_at_least_one_line = 0

    while (
        num_logs_pages_containing_at_least_one_line
        < MIN_LOG_PAGES_WITH_AT_LEAST_ONE_LINE
    ):
        time.sleep(15)

        logs_page = provisioner.get_worker_logs(
            worker_details=worker_details,
            max_lines=NUM_LOG_LINES_PER_PAGE,
            initial_offset=current_logs_offset,
        )

        print("Logs page:", logs_page)

        current_logs_offset = logs_page.next_offset

        if len(logs_page.log_lines) > 0:
            num_logs_pages_containing_at_least_one_line += 1

    provisioner.send_termination_signal_to_worker(worker_details=worker_details)

    while status == WorkerStatus.RUNNING:
        time.sleep(15)

        status = provisioner.check_worker_status(worker_details=worker_details)
        print("Status:", status)

    assert status == WorkerStatus.TERMINATED

    # Also check that if a worker has already been erased from ECS, then we report it as terminated.
    nonexistent_worker_details = WorkerDetails(
        type=WorkerType.AWS_ECS,
        identifier=worker_details.identifier[:-1]
        + ("a" if worker_details.identifier[-1] != "a" else "b"),
        logs_identifier=(
            worker_details.logs_identifier[:-1]
            + ("a" if worker_details.logs_identifier[-1] != "a" else "b")
        ),
    )

    status_of_nonexistent_worker = provisioner.check_worker_status(
        worker_details=nonexistent_worker_details
    )
    print("Status of non-existent worker:", status_of_nonexistent_worker)

    assert status_of_nonexistent_worker == WorkerStatus.TERMINATED

    logs_result_for_nonexistent_worker = provisioner.get_worker_logs(
        worker_details=nonexistent_worker_details, max_lines=3, initial_offset=None
    )
    print("Logs page for non-existent worker:", logs_result_for_nonexistent_worker)

    assert len(logs_result_for_nonexistent_worker.log_lines) == 0
    assert logs_result_for_nonexistent_worker.next_offset is None


if __name__ == "__main__":
    main()
