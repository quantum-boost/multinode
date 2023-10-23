import time
from pathlib import Path
from typing import Tuple
from unittest.mock import MagicMock, call

import jsonpickle
import pytest

from multinode.api_client import (
    ExecutionFinalResultPayload,
    ExecutionInfo,
    ExecutionOutcome,
    ExecutionSpec,
    ExecutionTemporaryResultPayload,
    FunctionStatus,
    ResourceSpec,
    WorkerStatus,
)
from multinode.core.errors import InvocationCancelledError
from multinode.worker.main import WorkerContext
from multinode.worker.runner import WorkerRunner

PROJECT_NAME = "test-project"
VERSION_ID = "test-version"
INVOCATION_ID = "test-invocation"
EXECUTION_ID = "test-execution"
RESOURCE_SPEC = ResourceSpec(
    virtual_cpus=1.0,
    memory_gbs=1.0,
    max_concurrency=10,
)
DEFAULT_EXECUTION_SPEC = ExecutionSpec(
    max_retries=3,
    timeout_seconds=3600,
)
FUNCTION_STATUS = FunctionStatus.READY
INPUT = jsonpickle.encode([("test-input",), {}])
TIME = 1


# Couldn't use fixtures because function_name differs between tests
def get_test_params(
    function_name: str,
) -> Tuple[MagicMock, WorkerContext, Path]:
    api_client = get_api_client(function_name)
    context = get_context(function_name)
    project_dir = get_project_dir()
    return api_client, context, project_dir


def get_api_client(function_name: str) -> MagicMock:
    curr_time = int(time.time())
    execution = ExecutionInfo(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        input=INPUT,
        cancellation_request_time=None,
        resource_spec=RESOURCE_SPEC,
        execution_spec=DEFAULT_EXECUTION_SPEC,
        function_status=FUNCTION_STATUS,
        prepared_function_details=None,
        worker_status=WorkerStatus.RUNNING,
        worker_details=None,
        termination_signal_time=None,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=curr_time,
        last_update_time=curr_time,
        execution_start_time=None,
        execution_finish_time=None,
        invocation_creation_time=curr_time,
    )

    api_client = MagicMock()
    api_client.get_execution = MagicMock(return_value=execution)
    api_client.start_execution = MagicMock()
    api_client.update_execution = MagicMock()
    api_client.finish_execution = MagicMock()
    return api_client


def get_project_dir() -> Path:
    return Path(__file__).parent / "test_project"


def get_context(function_name: str) -> WorkerContext:
    return WorkerContext(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
    )


def test_run_worker_return_function() -> None:
    function_name = "return_function"
    api_client, context, project_dir = get_test_params(function_name)
    runner = WorkerRunner(api_client, context, project_dir)
    runner.run_worker()

    api_client.start_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
    )
    api_client.update_execution.assert_not_called()
    api_client.finish_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        execution_final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.SUCCEEDED, final_output=jsonpickle.encode(10)
        ),
    )


@pytest.mark.parametrize(
    "function_name",
    # We do not respect return in yield functions so the results should be the same
    ["yield_function", "yield_function_with_return"],
)
def test_run_worker_yield_function(function_name) -> None:
    api_client, context, project_dir = get_test_params(function_name)
    runner = WorkerRunner(api_client, context, project_dir)
    runner.run_worker()

    # Has to be a relative import (like in the test_project)
    # for reference comparison to work
    from yield_fn_final import YieldFnFinal

    api_client.start_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
    )

    api_client.update_execution.assert_has_calls(
        [
            call(
                project_name=PROJECT_NAME,
                version_ref_str=VERSION_ID,
                function_name=function_name,
                invocation_id=INVOCATION_ID,
                execution_id=EXECUTION_ID,
                execution_temporary_result_payload=ExecutionTemporaryResultPayload(
                    latest_output=jsonpickle.encode({0: ""})
                ),
            ),
            call(
                project_name=PROJECT_NAME,
                version_ref_str=VERSION_ID,
                function_name=function_name,
                invocation_id=INVOCATION_ID,
                execution_id=EXECUTION_ID,
                execution_temporary_result_payload=ExecutionTemporaryResultPayload(
                    latest_output=jsonpickle.encode({6: "test-i"})
                ),
            ),
            call(
                project_name=PROJECT_NAME,
                version_ref_str=VERSION_ID,
                function_name=function_name,
                invocation_id=INVOCATION_ID,
                execution_id=EXECUTION_ID,
                execution_temporary_result_payload=ExecutionTemporaryResultPayload(
                    latest_output=jsonpickle.encode(
                        YieldFnFinal(
                            [
                                {0: ""},
                                {1: "t"},
                                {2: "te"},
                                {3: "tes"},
                                {4: "test"},
                                {5: "test-"},
                                {6: "test-i"},
                                {7: "test-in"},
                                {8: "test-inp"},
                                {9: "test-inpu"},
                            ]
                        )
                    )
                ),
            ),
        ]
    )
    assert len(api_client.update_execution.mock_calls) == 3

    api_client.finish_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        execution_final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.SUCCEEDED
        ),
    )


def test_run_worker_failed_yield_function() -> None:
    function_name = "failed_function"
    api_client, context, project_dir = get_test_params(function_name)
    runner = WorkerRunner(api_client, context, project_dir)

    # Has to be a relative import (like in the test_project)
    # for reference comparison to work
    from test_project.errors.FailedFunctionError import FailedFunctionError

    with pytest.raises(FailedFunctionError):
        runner.run_worker()

    api_client.start_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
    )
    api_client.update_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        execution_temporary_result_payload=ExecutionTemporaryResultPayload(
            latest_output=jsonpickle.encode("intermediate-output")
        ),
    )
    api_client.finish_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        execution_final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.FAILED,
            error_message="FailedFunctionError: I'm a failed function after all :(",
        ),
    )


def test_run_worker_handled_aborted_function() -> None:
    function_name = "handled_aborted_function"
    api_client, context, project_dir = get_test_params(function_name)
    runner = WorkerRunner(api_client, context, project_dir)
    runner.run_worker()

    api_client.start_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
    )
    api_client.update_execution.assert_has_calls(
        [
            call(
                project_name=PROJECT_NAME,
                version_ref_str=VERSION_ID,
                function_name=function_name,
                invocation_id=INVOCATION_ID,
                execution_id=EXECUTION_ID,
                execution_temporary_result_payload=ExecutionTemporaryResultPayload(
                    latest_output=jsonpickle.encode("pre-abort-output")
                ),
            ),
            call(
                project_name=PROJECT_NAME,
                version_ref_str=VERSION_ID,
                function_name=function_name,
                invocation_id=INVOCATION_ID,
                execution_id=EXECUTION_ID,
                execution_temporary_result_payload=ExecutionTemporaryResultPayload(
                    latest_output=jsonpickle.encode("cleanup-output")
                ),
            ),
        ]
    )
    assert len(api_client.update_execution.mock_calls) == 2

    api_client.finish_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        execution_final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.ABORTED
        ),
    )


def test_run_worker_unhandled_aborted_function() -> None:
    function_name = "unhandled_aborted_function"
    api_client, context, project_dir = get_test_params(function_name)
    runner = WorkerRunner(api_client, context, project_dir)

    with pytest.raises(InvocationCancelledError):
        runner.run_worker()

    api_client.start_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
    )
    api_client.update_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        execution_temporary_result_payload=ExecutionTemporaryResultPayload(
            latest_output=jsonpickle.encode("pre-abort-output")
        ),
    )

    api_client.finish_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        execution_final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.ABORTED
        ),
    )


def test_run_worker_failed_function_during_abort_handling() -> None:
    function_name = "failed_function_during_abort_handling"
    api_client, context, project_dir = get_test_params(function_name)
    runner = WorkerRunner(api_client, context, project_dir)

    # Has to be a relative import (like in the test_project)
    # for reference comparison to work
    from test_project.errors.FailedFunctionError import FailedFunctionError

    with pytest.raises(FailedFunctionError):
        runner.run_worker()

    api_client.start_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
    )
    api_client.update_execution.assert_has_calls(
        [
            call(
                project_name=PROJECT_NAME,
                version_ref_str=VERSION_ID,
                function_name=function_name,
                invocation_id=INVOCATION_ID,
                execution_id=EXECUTION_ID,
                execution_temporary_result_payload=ExecutionTemporaryResultPayload(
                    latest_output=jsonpickle.encode("pre-abort-output")
                ),
            ),
            call(
                project_name=PROJECT_NAME,
                version_ref_str=VERSION_ID,
                function_name=function_name,
                invocation_id=INVOCATION_ID,
                execution_id=EXECUTION_ID,
                execution_temporary_result_payload=ExecutionTemporaryResultPayload(
                    latest_output=jsonpickle.encode("cleanup-pre-failure-output")
                ),
            ),
        ]
    )
    assert len(api_client.update_execution.mock_calls) == 2

    api_client.finish_execution.assert_called_once_with(
        project_name=PROJECT_NAME,
        version_ref_str=VERSION_ID,
        function_name=function_name,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        execution_final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.FAILED,
            error_message="FailedFunctionError: I'm a failed function after all :(",
        ),
    )
