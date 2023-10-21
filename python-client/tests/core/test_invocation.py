import jsonpickle

from multinode.api_client import (
    ExecutionOutcome,
    ExecutionSpec,
    ExecutionSummary,
    FunctionStatus,
    InvocationInfo,
)
from multinode.api_client import InvocationStatus as ApiInvocationStatus
from multinode.api_client import ResourceSpec, WorkerStatus
from multinode.core.invocation import Invocation, InvocationStatus

PROJECT_NAME = "test-project"
VERSION_ID = "test-version"
FUNCTION_NAME = "test-function"
INVOCATION_ID = "test-invocation"
RESOURCE_SPEC = ResourceSpec(
    virtual_cpus=1.0,
    memory_gbs=1.0,
    max_concurrency=10,
)
DEFAULT_EXECUTION_SPEC = ExecutionSpec(
    max_retries=3,
    timeout_seconds=60,
)
STRICT_EXECUTION_SPEC = ExecutionSpec(
    max_retries=0,
    timeout_seconds=10,
)
FUNCTION_STATUS = FunctionStatus.READY
INPUT = jsonpickle.encode("test-input")
TIME = 1

FAILED_EXECUTION_WITH_OUTPUT = ExecutionSummary(
    execution_id="terminated-failed-execution1",
    worker_status=WorkerStatus.TERMINATED,
    worker_details=None,
    outcome=ExecutionOutcome.FAILED,
    output=jsonpickle.encode("intermediate-output"),
    error_message="test-error-message1",
    creation_time=TIME + 1,
    execution_start_time=TIME + 2,
    termination_signal_time=TIME + 3,
    last_update_time=TIME + 4,
    execution_finish_time=TIME + 4,
)

FAILED_EXECUTION_WITHOUT_OUTPUT = ExecutionSummary(
    execution_id="terminated-failed-execution2",
    worker_status=WorkerStatus.TERMINATED,
    worker_details=None,
    outcome=ExecutionOutcome.FAILED,
    output=None,
    error_message="test-error-message2",
    creation_time=TIME + 11,
    execution_start_time=TIME + 12,
    termination_signal_time=TIME + 13,
    last_update_time=TIME + 14,
    execution_finish_time=TIME + 14,
)

SUCCESSFUL_EXECUTION = ExecutionSummary(
    execution_id="terminated-successful-execution",
    worker_status=WorkerStatus.TERMINATED,
    worker_details=None,
    outcome=ExecutionOutcome.SUCCEEDED,
    output=jsonpickle.encode("test-output"),
    error_message=None,
    creation_time=TIME + 11,
    execution_start_time=TIME + 12,
    termination_signal_time=TIME + 13,
    last_update_time=TIME + 14,
    execution_finish_time=TIME + 14,
)

ABORTED_EXECUTION = ExecutionSummary(
    execution_id="aborted-execution",
    worker_status=WorkerStatus.TERMINATED,
    worker_details=None,
    outcome=ExecutionOutcome.ABORTED,
    output=jsonpickle.encode("aborted-output"),
    error_message=None,
    creation_time=TIME + 11,
    execution_start_time=TIME + 12,
    termination_signal_time=TIME + 13,
    last_update_time=TIME + 14,
    execution_finish_time=TIME + 14,
)

RUNNING_EXECUTION = ExecutionSummary(
    execution_id="running-execution",
    worker_status=WorkerStatus.RUNNING,
    worker_details=None,
    outcome=None,
    output=None,
    error_message=None,
    creation_time=TIME,
    execution_start_time=TIME + 5,
    termination_signal_time=None,
    last_update_time=TIME + 6,
    execution_finish_time=None,
)

PENDING_EXECUTION = ExecutionSummary(
    execution_id="pending-execution",
    worker_status=WorkerStatus.PENDING,
    worker_details=None,
    outcome=None,
    output=None,
    error_message=None,
    creation_time=TIME,
    execution_start_time=None,
    termination_signal_time=None,
    last_update_time=TIME + 1,
    execution_finish_time=None,
)


def test_from_invocation_info_with_successful_execution_and_1_retry() -> None:
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        cancellation_request_time=None,
        invocation_status=ApiInvocationStatus.TERMINATED,
        creation_time=TIME,
        last_update_time=TIME + 30,
        executions=[FAILED_EXECUTION_WITH_OUTPUT, SUCCESSFUL_EXECUTION],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.SUCCEEDED
    assert invocation.result == "test-output"
    assert invocation.error is None
    assert invocation.terminated is True
    assert invocation.num_failed_attempts == 1


def test_from_invocation_info_with_successful_execution_after_timeout() -> None:
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=STRICT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        # Succeeded should take priority over cancellations
        cancellation_request_time=TIME,
        invocation_status=ApiInvocationStatus.TERMINATED,
        creation_time=TIME,
        last_update_time=TIME + 15,
        executions=[SUCCESSFUL_EXECUTION],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.SUCCEEDED
    assert invocation.result == "test-output"
    assert invocation.error is None
    assert invocation.terminated is True
    assert invocation.num_failed_attempts == 0


def test_from_invocation_info_with_different_failed_executions_before_retries_limit() -> (
    None
):
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        cancellation_request_time=None,
        invocation_status=ApiInvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME + 15,
        executions=[
            FAILED_EXECUTION_WITH_OUTPUT,
            FAILED_EXECUTION_WITHOUT_OUTPUT,
            FAILED_EXECUTION_WITH_OUTPUT,
        ],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    # Should be pending because there are still retries left
    assert invocation.status == InvocationStatus.PENDING
    # And should pick up results from the execution with the most recent update time
    assert invocation.result is None
    assert invocation.error == "test-error-message2"
    assert invocation.terminated is False
    assert invocation.num_failed_attempts == 3


def test_from_invocation_info_with_all_executions_failed_after_retries_limit() -> None:
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        # Failed should take priority over cancellations
        cancellation_request_time=TIME,
        invocation_status=ApiInvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME + 15,
        executions=[
            FAILED_EXECUTION_WITH_OUTPUT,
            FAILED_EXECUTION_WITH_OUTPUT,
            FAILED_EXECUTION_WITH_OUTPUT,
            FAILED_EXECUTION_WITH_OUTPUT,
        ],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.FAILED
    assert invocation.result == "intermediate-output"
    assert invocation.error == "test-error-message1"
    assert invocation.terminated is False
    assert invocation.num_failed_attempts == 4


def test_from_invocation_info_with_aborted_execution() -> None:
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        cancellation_request_time=TIME,
        invocation_status=ApiInvocationStatus.TERMINATED,
        creation_time=TIME,
        last_update_time=TIME + 15,
        executions=[
            FAILED_EXECUTION_WITH_OUTPUT,
            FAILED_EXECUTION_WITH_OUTPUT,
            FAILED_EXECUTION_WITH_OUTPUT,
            ABORTED_EXECUTION,
        ],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.CANCELLED
    assert invocation.result == "aborted-output"
    assert invocation.error is None
    assert invocation.terminated is True
    assert invocation.num_failed_attempts == 3


def test_from_invocation_info_with_aborted_execution_after_timeout() -> None:
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=STRICT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        cancellation_request_time=TIME,
        invocation_status=ApiInvocationStatus.TERMINATED,
        creation_time=TIME,
        last_update_time=TIME + 15,
        executions=[ABORTED_EXECUTION],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.TIMED_OUT
    assert invocation.result == "aborted-output"
    assert invocation.error is None
    assert invocation.terminated is True
    assert invocation.num_failed_attempts == 0


def test_from_invocation_info_with_running_execution_and_cancellation_request() -> None:
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        cancellation_request_time=TIME,
        invocation_status=ApiInvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME + 5,
        executions=[RUNNING_EXECUTION],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.CANCELLING
    assert invocation.result is None
    assert invocation.error is None
    assert invocation.terminated is False
    assert invocation.num_failed_attempts == 0


def test_from_invocation_info_with_running_and_failed_execution() -> None:
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        cancellation_request_time=None,
        invocation_status=ApiInvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME + 5,
        executions=[FAILED_EXECUTION_WITH_OUTPUT, RUNNING_EXECUTION],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.RUNNING
    # Should pick up running execution's outputs
    assert invocation.result is None
    assert invocation.error is None
    assert invocation.terminated is False
    assert invocation.num_failed_attempts == 1


def test_from_invocation_info_without_executions() -> None:
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        cancellation_request_time=None,
        invocation_status=ApiInvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME,
        executions=[],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.PENDING
    assert invocation.result is None
    assert invocation.error is None
    assert invocation.terminated is False
    assert invocation.num_failed_attempts == 0


def test_from_invocation_info_with_pending_execution() -> None:
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        cancellation_request_time=None,
        invocation_status=ApiInvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME,
        executions=[PENDING_EXECUTION],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.PENDING
    assert invocation.result is None
    assert invocation.error is None
    assert invocation.terminated is False
    assert invocation.num_failed_attempts == 0


def test_from_invocation_info_without_executions_but_terminated_invocation() -> None:
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        cancellation_request_time=None,
        invocation_status=ApiInvocationStatus.TERMINATED,
        creation_time=TIME,
        last_update_time=TIME,
        executions=[],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.CANCELLED
    assert invocation.result is None
    assert invocation.error is None
    assert invocation.terminated is True
    assert invocation.num_failed_attempts == 0


def test_from_invocation_info_with_failed_executions_before_retries_limit_but_terminated_invocation() -> (  # noqa: B950
    None
):
    inv_info = InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        input=INPUT,
        cancellation_request_time=None,
        invocation_status=ApiInvocationStatus.TERMINATED,
        creation_time=TIME,
        last_update_time=TIME,
        executions=[FAILED_EXECUTION_WITH_OUTPUT, FAILED_EXECUTION_WITH_OUTPUT],
    )

    invocation = Invocation.from_invocation_info(inv_info)

    assert invocation.status == InvocationStatus.CANCELLED
    assert invocation.result == "intermediate-output"
    assert invocation.error == "test-error-message1"
    assert invocation.terminated is True
    assert invocation.num_failed_attempts == 2
