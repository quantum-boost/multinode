from typing import Optional

from control_plane.control.periodic.invocations_helper import (
    RunningInvocationsClassification,
    classify_running_invocations,
)
from control_plane.types.datatypes import (
    ExecutionOutcome,
    ExecutionSpec,
    ExecutionSummary,
    FunctionInfo,
    FunctionStatus,
    InvocationInfo,
    InvocationStatus,
    PreparedFunctionDetails,
    ResourceSpec,
    WorkerStatus,
    WorkerType,
)
from control_plane.types.random_ids import generate_random_id

PROJECT_NAME = "project"
VERSION_ID = "version"
FUNCTION_NAME = "function"
NOT_READY_FUNCTION_NAME = "other_function"

RESOURCE_SPEC = ResourceSpec(virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=2)
EXECUTION_SPEC = ExecutionSpec(max_retries=5, timeout_seconds=100)

TIME = 0

READY_FUNCTIONS: list[FunctionInfo] = [
    FunctionInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        docker_image="image:latest",
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=FunctionStatus.READY,
        prepared_function_details=PreparedFunctionDetails(
            type=WorkerType.TEST, identifier=""
        ),
    )
]


def create_execution(
    worker_status: WorkerStatus, outcome: Optional[ExecutionOutcome]
) -> ExecutionSummary:
    # Not all the fields are realistic, but this doesn't matter for the test.
    return ExecutionSummary(
        execution_id=generate_random_id("exe"),
        worker_status=worker_status,
        worker_details=None,
        termination_signal_sent=False,
        outcome=outcome,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=None,
        execution_finish_time=None,
    )


def create_invocation(
    invocation_id: str,
    function_name: str,
    cancellation_requested: bool = False,
    creation_time: int = TIME,
    function_status: FunctionStatus = FunctionStatus.READY,
    num_pending_executions: int = 0,
    num_running_executions: int = 0,
    num_successful_terminated_executions: int = 0,
    num_aborted_terminated_executions: int = 0,
    num_failed_terminated_executions: int = 0,
    num_terminated_executions_without_outcome: int = 0,
) -> InvocationInfo:
    executions: list[ExecutionSummary] = []

    for _ in range(num_pending_executions):
        executions.append(create_execution(WorkerStatus.PENDING, None))

    for _ in range(num_running_executions):
        executions.append(create_execution(WorkerStatus.RUNNING, None))

    for _ in range(num_successful_terminated_executions):
        executions.append(
            create_execution(WorkerStatus.TERMINATED, ExecutionOutcome.SUCCEEDED)
        )

    for _ in range(num_aborted_terminated_executions):
        executions.append(
            create_execution(WorkerStatus.TERMINATED, ExecutionOutcome.ABORTED)
        )

    for _ in range(num_failed_terminated_executions):
        executions.append(
            create_execution(WorkerStatus.TERMINATED, ExecutionOutcome.FAILED)
        )

    for _ in range(num_terminated_executions_without_outcome):
        executions.append(create_execution(WorkerStatus.TERMINATED, None))

    return InvocationInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=function_name,
        invocation_id=invocation_id,
        parent_invocation=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=function_status,
        prepared_function_details=None,
        input="input",
        cancellation_requested=cancellation_requested,
        invocation_status=InvocationStatus.RUNNING,
        creation_time=creation_time,
        last_update_time=TIME,
        executions=executions,
    )


def assert_results(
    classification: RunningInvocationsClassification,
    expected_ids_to_terminate: Optional[set[str]] = None,
    expected_ids_to_create_executions_for: Optional[set[str]] = None,
    expected_ids_to_leave_untouched: Optional[set[str]] = None,
) -> None:
    actual_ids_to_terminate = {
        invocation.invocation_id
        for invocation in classification.invocations_to_terminate
    }
    actual_ids_to_create_executions_for = {
        invocation.invocation_id
        for invocation in classification.invocations_to_create_executions_for
    }
    actual_ids_to_leave_untouched = {
        invocation.invocation_id
        for invocation in classification.invocations_to_leave_untouched
    }

    if expected_ids_to_terminate is None:
        assert len(actual_ids_to_terminate) == 0
    else:
        assert actual_ids_to_terminate == expected_ids_to_terminate

    if expected_ids_to_create_executions_for is None:
        assert len(actual_ids_to_create_executions_for) == 0
    else:
        assert (
            actual_ids_to_create_executions_for == expected_ids_to_create_executions_for
        )

    if expected_ids_to_leave_untouched is None:
        assert len(actual_ids_to_leave_untouched) == 0
    else:
        assert actual_ids_to_leave_untouched == expected_ids_to_leave_untouched


def test_classify_with_an_existing_running_execution() -> None:
    invocation_id = "inv-1"
    running_invocations = [
        create_invocation(invocation_id, FUNCTION_NAME, num_running_executions=1)
    ]

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(classification, expected_ids_to_leave_untouched={invocation_id})


def test_classify_with_an_existing_pending_execution() -> None:
    invocation_id = "inv-1"
    running_invocations = [
        create_invocation(invocation_id, FUNCTION_NAME, num_pending_executions=1)
    ]

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(classification, expected_ids_to_leave_untouched={invocation_id})


def test_classify_with_cancellation_request() -> None:
    invocation_id = "inv-1"
    running_invocations = [
        create_invocation(invocation_id, FUNCTION_NAME, cancellation_requested=True)
    ]

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(classification, expected_ids_to_terminate={invocation_id})


def test_classify_when_timed_out() -> None:
    invocation_id = "inv-1"
    running_invocations = [
        create_invocation(
            invocation_id,
            FUNCTION_NAME,
            creation_time=(TIME - EXECUTION_SPEC.timeout_seconds - 10),
        )
    ]

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(classification, expected_ids_to_terminate={invocation_id})


def test_classify_with_max_retries_reached() -> None:
    invocation_id = "inv-1"
    running_invocations = [
        create_invocation(
            invocation_id,
            FUNCTION_NAME,
            num_failed_terminated_executions=(EXECUTION_SPEC.max_retries + 1),
        )
    ]

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(classification, expected_ids_to_terminate={invocation_id})


def test_classify_with_no_existing_executions() -> None:
    invocation_id = "inv-1"
    running_invocations = [create_invocation(invocation_id, FUNCTION_NAME)]

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(
        classification, expected_ids_to_create_executions_for={invocation_id}
    )


def test_classify_with_execution_failing_but_not_reaching_retries_limit() -> None:
    invocation_id = "inv-1"
    running_invocations = [
        create_invocation(
            invocation_id, FUNCTION_NAME, num_failed_terminated_executions=1
        )
    ]

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(
        classification, expected_ids_to_create_executions_for={invocation_id}
    )


def test_classify_with_execution_suffering_hardware_failure_but_not_reaching_retries_limit() -> (
    None
):
    invocation_id = "inv-1"
    running_invocations = [
        create_invocation(
            invocation_id, FUNCTION_NAME, num_terminated_executions_without_outcome=1
        )
    ]

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(
        classification, expected_ids_to_create_executions_for={invocation_id}
    )


def test_classify_with_function_not_in_ready_status() -> None:
    invocation_id = "inv-1"
    running_invocations = [
        create_invocation(
            invocation_id,
            NOT_READY_FUNCTION_NAME,
            function_status=FunctionStatus.PENDING,
        )
    ]

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(classification, expected_ids_to_leave_untouched={invocation_id})


def test_classify_with_function_capacity_already_saturated() -> None:
    invocation_id_1 = "inv-1"  # running an execution
    invocation_id_2 = "inv-2"  # running an execution
    invocation_id_3 = "inv-3"  # not yet running an execution
    running_invocations = [
        create_invocation(invocation_id_1, FUNCTION_NAME, num_running_executions=1),
        create_invocation(invocation_id_2, FUNCTION_NAME, num_running_executions=1),
        create_invocation(invocation_id_3, FUNCTION_NAME),
    ]

    # ... but max_concurrency is 2, so we can't create an execution for inv-3.

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(
        classification,
        expected_ids_to_leave_untouched={
            invocation_id_1,
            invocation_id_2,
            invocation_id_3,
        },
    )


def test_classify_with_function_capacity_partially_used_but_not_saturated() -> None:
    invocation_id_1 = "inv-1"  # running an execution
    invocation_id_2 = "inv-2"  # not yet running an execution
    running_invocations = [
        create_invocation(invocation_id_1, FUNCTION_NAME, num_running_executions=1),
        create_invocation(invocation_id_2, FUNCTION_NAME),
    ]

    # With max_concurrency is 2, we *can* create an execution for inv-2.

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert_results(
        classification,
        expected_ids_to_leave_untouched={invocation_id_1},
        expected_ids_to_create_executions_for={invocation_id_2},
    )


def test_classify_with_function_capacity_enough_to_create_execution_for_some_but_not_all_invocations() -> (
    None
):
    invocation_id_1 = "inv-1"  # running an execution
    invocation_id_2 = "inv-2"  # not yet running an execution
    invocation_id_3 = "inv-3"  # not yet running an execution
    running_invocations = [
        create_invocation(invocation_id_1, FUNCTION_NAME, num_running_executions=1),
        create_invocation(invocation_id_2, FUNCTION_NAME),
        create_invocation(invocation_id_3, FUNCTION_NAME),
    ]

    # With max_concurrency is 2, we can create an execution for invocation_2 or invocation_3, but not both!

    classification = classify_running_invocations(
        running_invocations, READY_FUNCTIONS, TIME
    )
    assert len(classification.invocations_to_leave_untouched) == 2
    assert len(classification.invocations_to_create_executions_for) == 1
    id_of_invocation_to_create_execution_for = (
        classification.invocations_to_create_executions_for[0].invocation_id
    )
    assert id_of_invocation_to_create_execution_for in {
        invocation_id_2,
        invocation_id_3,
    }
