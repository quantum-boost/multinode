from typing import Optional

from control_plane.control.periodic.executions_helper import (
    RunningExecutionsClassification,
    classify_running_executions,
)
from control_plane.types.datatypes import (
    ResourceSpec,
    ExecutionSpec,
    ExecutionInfo,
    FunctionStatus,
    WorkerStatus,
)

PROJECT_NAME = "project"
VERSION_ID = "version"
FUNCTION_NAME = "function"
INVOCATION_NAME = "invocation"

RESOURCE_SPEC = ResourceSpec(virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=2)
EXECUTION_SPEC = ExecutionSpec(max_retries=5, timeout_seconds=100)

TIME = 0


def create_execution(
    execution_id: str,
    cancellation_requested: bool = False,
    termination_signal_already_sent: bool = False,
    invocation_creation_time: int = TIME,
) -> ExecutionInfo:
    return ExecutionInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_NAME,
        execution_id=execution_id,
        input="input",
        cancellation_requested=cancellation_requested,
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=FunctionStatus.READY,
        prepared_function_details=None,
        worker_status=WorkerStatus.RUNNING,
        worker_details=None,
        termination_signal_sent=termination_signal_already_sent,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=TIME,
        execution_finish_time=TIME,
        invocation_creation_time=invocation_creation_time,
    )


def assert_results(
    classification: RunningExecutionsClassification,
    expected_ids_requiring_termination_signal: Optional[set[str]] = None,
    expected_ids_to_leave_untouched: Optional[set[str]] = None,
) -> None:
    actual_ids_requiring_termination_signal = {
        execution.execution_id for execution in classification.executions_requiring_termination_signal
    }
    actual_ids_to_leave_untouched = {
        execution.execution_id for execution in classification.executions_to_leave_untouched
    }

    if expected_ids_requiring_termination_signal is None:
        assert len(actual_ids_requiring_termination_signal) == 0
    else:
        assert actual_ids_requiring_termination_signal == expected_ids_requiring_termination_signal

    if expected_ids_to_leave_untouched is None:
        assert len(actual_ids_to_leave_untouched) == 0
    else:
        assert actual_ids_to_leave_untouched == expected_ids_to_leave_untouched


def test_classify_in_standard_case() -> None:
    execution_id = "exe-1"
    executions = [create_execution(execution_id)]

    classification = classify_running_executions(executions, TIME)

    assert_results(classification, expected_ids_to_leave_untouched={execution_id})


def test_classify_with_cancellation_request() -> None:
    execution_id = "exe-1"
    executions = [create_execution(execution_id, cancellation_requested=True)]

    classification = classify_running_executions(executions, TIME)

    assert_results(classification, expected_ids_requiring_termination_signal={execution_id})


def test_classify_with_cancellation_request_but_termination_signal_already_sent() -> None:
    execution_id = "exe-1"
    executions = [
        create_execution(
            execution_id,
            cancellation_requested=True,
            termination_signal_already_sent=True,
        )
    ]

    classification = classify_running_executions(executions, TIME)

    assert_results(classification, expected_ids_to_leave_untouched={execution_id})


def test_classify_when_timed_out() -> None:
    execution_id = "exe-1"
    executions = [
        create_execution(
            execution_id,
            invocation_creation_time=(TIME - EXECUTION_SPEC.timeout_seconds - 50),
        )
    ]

    classification = classify_running_executions(executions, TIME)

    assert_results(classification, expected_ids_requiring_termination_signal={execution_id})


def test_classify_when_timed_out_but_termination_signal_already_sent() -> None:
    execution_id = "exe-1"
    executions = [
        create_execution(
            execution_id,
            invocation_creation_time=(TIME - EXECUTION_SPEC.timeout_seconds - 50),
            termination_signal_already_sent=True,
        )
    ]

    classification = classify_running_executions(executions, TIME)

    assert_results(classification, expected_ids_to_leave_untouched={execution_id})


def test_classify_with_more_than_one_execution_in_list() -> None:
    execution_id_1 = "exe-1"
    execution_id_2 = "exe-2"
    executions = [
        create_execution(execution_id_1),
        create_execution(execution_id_2, cancellation_requested=True),
    ]

    classification = classify_running_executions(executions, TIME)

    assert_results(
        classification,
        expected_ids_to_leave_untouched={execution_id_1},
        expected_ids_requiring_termination_signal={execution_id_2},
    )
