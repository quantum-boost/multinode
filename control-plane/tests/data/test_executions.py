from typing import Iterable

import pytest

from control_plane.data.data_store import DataStore
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import (
    ExecutionAlreadyExists,
    ExecutionDoesNotExist,
    ExecutionHasAlreadyFinished,
    ExecutionHasAlreadyStarted,
    ExecutionHasNotStarted,
    FunctionDoesNotExist,
    InvocationDoesNotExist,
    ProjectDoesNotExist,
    VersionDoesNotExist,
)
from control_plane.types.datatypes import (
    ExecutionOutcome,
    ExecutionSpec,
    FunctionStatus,
    InvocationStatus,
    PreparedFunctionDetails,
    ResourceSpec,
    WorkerDetails,
    WorkerStatus,
    WorkerType,
)


@pytest.fixture(scope="module")
def conn_pool() -> Iterable[SqlConnectionPool]:
    conn_pool = SqlConnectionPool.create_for_local_postgres()
    try:
        yield conn_pool
    finally:
        conn_pool.close()


PROJECT_NAME = "project-1"
NONEXISTENT_PROJECT_NAME = "nonexistent-project"

VERSION_ID = "version-1"
NONEXISTENT_VERSION_NAME = "nonexistent-version"

FUNCTION_NAME_1 = "function-1"
FUNCTION_NAME_2 = "function-2"
NONEXISTENT_FUNCTION_NAME = "nonexistent-function"

DOCKER_IMAGE_1 = "image-1"
DOCKER_IMAGE_2 = "image-2"
RESOURCE_SPEC_1 = ResourceSpec(virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=5)
RESOURCE_SPEC_2 = ResourceSpec(virtual_cpus=4.0, memory_gbs=16.0, max_concurrency=5)
EXECUTION_SPEC_1 = ExecutionSpec(timeout_seconds=300, max_retries=8)
EXECUTION_SPEC_2 = ExecutionSpec(timeout_seconds=600, max_retries=8)
PREPARED_FUNCTION_DETAILS_1 = PreparedFunctionDetails(
    type=WorkerType.TEST, identifier="def-1"
)
PREPARED_FUNCTION_DETAILS_2 = PreparedFunctionDetails(
    type=WorkerType.TEST, identifier="def-2"
)

INVOCATION_ID_1 = "invocation-1"
INVOCATION_ID_2 = "invocation-2"
NONEXISTENT_INVOCATION_ID = "nonexistent-invocation"

INPUT_1 = "input-1"
INPUT_2 = "input-2"
OUTPUT_1 = "output-1"
OUTPUT_2 = "output-2"
ERROR_MESSAGE = "error"

EXECUTION_ID_1 = "execution-1"
EXECUTION_ID_2 = "execution-2"

WORKER_DETAILS = WorkerDetails(
    type=WorkerType.TEST, identifier="worker", logs_identifier="logs"
)

PROJECT_CREATION_TIME = -20
INVOCATION_CREATION_TIME_1 = -10
INVOCATION_CREATION_TIME_2 = -5
TIME = 0
LATER_TIME = 10


@pytest.fixture()
def data_store(conn_pool: SqlConnectionPool) -> Iterable[DataStore]:
    data_store = DataStore(conn_pool)
    data_store.create_tables()

    # Set up each test with the project, version, functions and invocations already inserted.
    # Note that the two invocations are associated with different functions.
    data_store.projects.create(
        project_name=PROJECT_NAME,
        deletion_request_time=None,
        creation_time=PROJECT_CREATION_TIME,
    )

    data_store.project_versions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        creation_time=PROJECT_CREATION_TIME,
    )

    data_store.functions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        docker_image=DOCKER_IMAGE_1,
        resource_spec=RESOURCE_SPEC_1,
        execution_spec=EXECUTION_SPEC_1,
        function_status=FunctionStatus.READY,
        prepared_function_details=PREPARED_FUNCTION_DETAILS_1,
    )
    data_store.functions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        docker_image=DOCKER_IMAGE_2,
        resource_spec=RESOURCE_SPEC_2,
        execution_spec=EXECUTION_SPEC_2,
        function_status=FunctionStatus.READY,
        prepared_function_details=PREPARED_FUNCTION_DETAILS_2,
    )

    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        parent_invocation=None,
        input=INPUT_1,
        cancellation_request_time=None,
        invocation_status=InvocationStatus.RUNNING,
        creation_time=INVOCATION_CREATION_TIME_1,
        last_update_time=INVOCATION_CREATION_TIME_1,
    )
    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        invocation_id=INVOCATION_ID_2,
        parent_invocation=None,
        input=INPUT_2,
        cancellation_request_time=INVOCATION_CREATION_TIME_2,
        invocation_status=InvocationStatus.TERMINATED,
        creation_time=INVOCATION_CREATION_TIME_2,
        last_update_time=INVOCATION_CREATION_TIME_2,
    )

    try:
        yield data_store

    finally:
        data_store.delete_tables()


def test_create_two_executions_for_different_invocations(data_store: DataStore) -> None:
    # To begin with, no executions exist.
    with pytest.raises(ExecutionDoesNotExist):
        data_store.executions.get(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
        )

    all_executions = data_store.executions.list_all(
        worker_statuses={WorkerStatus.PENDING}
    )
    assert len(all_executions) == 0

    executions_for_invocation_1 = data_store.executions.list_for_invocation(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    ).executions
    assert len(executions_for_invocation_1) == 0

    # Create first execution
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        worker_status=WorkerStatus.PENDING,
        worker_details=None,
        termination_signal_sent=False,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=None,
        execution_finish_time=None,
    )

    execution = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
    )
    assert execution.project_name == PROJECT_NAME
    assert execution.version_id == VERSION_ID
    assert execution.function_name == FUNCTION_NAME_1
    assert execution.invocation_id == INVOCATION_ID_1
    assert execution.execution_id == EXECUTION_ID_1
    assert execution.input == INPUT_1
    assert execution.cancellation_requested == False
    assert execution.resource_spec.virtual_cpus == pytest.approx(
        RESOURCE_SPEC_1.virtual_cpus, 1.0e-5
    )
    assert execution.execution_spec == EXECUTION_SPEC_1
    assert execution.function_status == FunctionStatus.READY
    assert execution.prepared_function_details == PREPARED_FUNCTION_DETAILS_1
    assert execution.worker_status == WorkerStatus.PENDING
    assert execution.worker_details is None
    assert execution.termination_signal_sent == False
    assert execution.outcome is None
    assert execution.output is None
    assert execution.error_message is None
    assert execution.creation_time == TIME
    assert execution.last_update_time == TIME
    assert execution.execution_start_time is None
    assert execution.execution_finish_time is None
    assert execution.invocation_creation_time == INVOCATION_CREATION_TIME_1

    all_executions = data_store.executions.list_all(
        worker_statuses={WorkerStatus.PENDING}
    )
    assert len(all_executions) == 1
    assert all_executions[0].project_name == PROJECT_NAME
    assert all_executions[0].version_id == VERSION_ID
    assert all_executions[0].function_name == FUNCTION_NAME_1
    assert all_executions[0].invocation_id == INVOCATION_ID_1
    assert all_executions[0].execution_id == EXECUTION_ID_1
    assert all_executions[0].input == INPUT_1
    assert all_executions[0].cancellation_requested == False
    assert all_executions[0].resource_spec.virtual_cpus == pytest.approx(
        RESOURCE_SPEC_1.virtual_cpus, 1.0e-5
    )
    assert all_executions[0].execution_spec == EXECUTION_SPEC_1
    assert all_executions[0].function_status == FunctionStatus.READY
    assert all_executions[0].prepared_function_details == PREPARED_FUNCTION_DETAILS_1
    assert all_executions[0].worker_status == WorkerStatus.PENDING
    assert all_executions[0].worker_details is None
    assert all_executions[0].termination_signal_sent == False
    assert all_executions[0].outcome is None
    assert all_executions[0].output is None
    assert all_executions[0].error_message is None
    assert all_executions[0].creation_time == TIME
    assert all_executions[0].last_update_time == TIME
    assert all_executions[0].execution_start_time is None
    assert all_executions[0].execution_finish_time is None
    assert all_executions[0].invocation_creation_time == INVOCATION_CREATION_TIME_1

    executions_for_invocation_1 = data_store.executions.list_for_invocation(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    ).executions

    assert len(executions_for_invocation_1) == 1
    assert executions_for_invocation_1[0].execution_id == EXECUTION_ID_1
    assert executions_for_invocation_1[0].worker_status == WorkerStatus.PENDING
    assert executions_for_invocation_1[0].worker_details is None
    assert executions_for_invocation_1[0].termination_signal_sent == False
    assert executions_for_invocation_1[0].outcome is None
    assert executions_for_invocation_1[0].output is None
    assert executions_for_invocation_1[0].error_message is None
    assert executions_for_invocation_1[0].creation_time == TIME
    assert executions_for_invocation_1[0].last_update_time == TIME
    assert executions_for_invocation_1[0].execution_start_time is None
    assert executions_for_invocation_1[0].execution_finish_time is None

    invocation_1 = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    )
    assert len(invocation_1.executions) == 1
    assert invocation_1.executions[0].execution_id == EXECUTION_ID_1
    assert invocation_1.executions[0].worker_status == WorkerStatus.PENDING
    assert invocation_1.executions[0].worker_details is None
    assert invocation_1.executions[0].termination_signal_sent == False
    assert invocation_1.executions[0].outcome is None
    assert invocation_1.executions[0].output is None
    assert invocation_1.executions[0].error_message is None
    assert invocation_1.executions[0].creation_time == TIME
    assert invocation_1.executions[0].last_update_time == TIME
    assert invocation_1.executions[0].execution_start_time is None
    assert invocation_1.executions[0].execution_finish_time is None

    # Create another execution, but for another invocation
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        invocation_id=INVOCATION_ID_2,
        execution_id=EXECUTION_ID_2,
        worker_status=WorkerStatus.PENDING,
        worker_details=None,
        termination_signal_sent=False,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=LATER_TIME,
        last_update_time=LATER_TIME,
        execution_start_time=None,
        execution_finish_time=None,
    )

    execution_1 = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
    )
    assert execution_1.execution_id == EXECUTION_ID_1

    execution_2 = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        invocation_id=INVOCATION_ID_2,
        execution_id=EXECUTION_ID_2,
    )
    assert execution_2.execution_id == EXECUTION_ID_2
    assert execution_2.function_name == FUNCTION_NAME_2
    assert execution_2.execution_spec == EXECUTION_SPEC_2
    assert execution_2.prepared_function_details == PREPARED_FUNCTION_DETAILS_2

    all_executions = data_store.executions.list_all(
        worker_statuses={WorkerStatus.PENDING}
    )
    assert len(all_executions) == 2

    executions_for_invocation_1 = data_store.executions.list_for_invocation(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    ).executions
    assert len(executions_for_invocation_1) == 1
    assert executions_for_invocation_1[0].execution_id == EXECUTION_ID_1

    executions_for_invocation_2 = data_store.executions.list_for_invocation(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        invocation_id=INVOCATION_ID_2,
    ).executions
    assert len(executions_for_invocation_2) == 1
    assert executions_for_invocation_2[0].execution_id == EXECUTION_ID_2

    invocation_1 = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    )
    assert len(invocation_1.executions) == 1
    assert invocation_1.executions[0].execution_id == EXECUTION_ID_1

    invocation_2 = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        invocation_id=INVOCATION_ID_2,
    )
    assert len(invocation_2.executions) == 1
    assert invocation_2.executions[0].execution_id == EXECUTION_ID_2

    # Mismatch in invocation ID and execution ID => get error
    with pytest.raises(ExecutionDoesNotExist):
        data_store.executions.get(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,  # wrong invocation for this execution
            execution_id=EXECUTION_ID_2,
        )


def test_create_execution_with_duplicate_id(data_store: DataStore) -> None:
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        worker_status=WorkerStatus.PENDING,
        worker_details=None,
        termination_signal_sent=False,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=None,
        execution_finish_time=None,
    )

    # Try to create another execution, but with the same ID
    with pytest.raises(ExecutionAlreadyExists):
        data_store.executions.create(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            worker_status=WorkerStatus.PENDING,
            worker_details=None,
            termination_signal_sent=False,
            outcome=None,
            output=None,
            error_message=None,
            creation_time=LATER_TIME,
            last_update_time=LATER_TIME,
            execution_start_time=None,
            execution_finish_time=None,
        )

    execution = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
    )

    assert execution.creation_time == TIME  # the original value


def test_update_execution_status(data_store: DataStore) -> None:
    # Create the execution that we will update
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        worker_status=WorkerStatus.PENDING,
        worker_details=None,
        termination_signal_sent=False,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=None,
        execution_finish_time=None,
    )

    # Create another execution that should be left untouched
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_2,
        worker_status=WorkerStatus.PENDING,
        worker_details=None,
        termination_signal_sent=False,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=None,
        execution_finish_time=None,
    )

    # Update the status and the worker details for the first execution
    data_store.executions.update(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        update_time=LATER_TIME,
        new_worker_status=WorkerStatus.RUNNING,
        new_worker_details=WORKER_DETAILS,
    )

    # The first execution should have been updated
    execution_1 = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
    )
    assert execution_1.worker_status == WorkerStatus.RUNNING
    assert execution_1.worker_details == WORKER_DETAILS

    # The second execution should left untouched
    execution_2 = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_2,
    )
    assert execution_2.worker_status == WorkerStatus.PENDING
    assert execution_2.worker_details is None

    executions_in_pending_status = data_store.executions.list_all(
        worker_statuses={WorkerStatus.PENDING}
    )
    assert len(executions_in_pending_status) == 1
    assert executions_in_pending_status[0].execution_id == EXECUTION_ID_2

    executions_in_running_status = data_store.executions.list_all(
        worker_statuses={WorkerStatus.RUNNING}
    )
    assert len(executions_in_running_status) == 1
    assert executions_in_running_status[0].execution_id == EXECUTION_ID_1
    assert executions_in_running_status[0].worker_details == WORKER_DETAILS


def test_update_termination_sent_flag(data_store: DataStore) -> None:
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        worker_status=WorkerStatus.PENDING,
        worker_details=None,
        termination_signal_sent=False,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=None,
        execution_finish_time=None,
    )

    data_store.executions.update(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        update_time=LATER_TIME,
        set_termination_signal_sent=True,
    )

    execution = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
    )
    assert execution.termination_signal_sent == True


def test_update_execution_start_time(data_store: DataStore) -> None:
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        worker_status=WorkerStatus.RUNNING,
        worker_details=WORKER_DETAILS,
        termination_signal_sent=False,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=None,
        execution_finish_time=None,
    )

    data_store.executions.update(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        update_time=LATER_TIME,
        new_execution_start_time=LATER_TIME,
        should_already_have_started=False,
    )

    execution = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
    )
    assert execution.execution_start_time == LATER_TIME

    all_executions = data_store.executions.list_all(
        worker_statuses={WorkerStatus.RUNNING}
    )
    assert len(all_executions) == 1
    assert all_executions[0].execution_start_time == LATER_TIME

    executions_for_invocation = data_store.executions.list_for_invocation(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    ).executions
    assert len(executions_for_invocation) == 1
    assert executions_for_invocation[0].execution_start_time == LATER_TIME

    invocation = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    )
    assert len(invocation.executions) == 1
    assert invocation.executions[0].execution_start_time == LATER_TIME


def test_update_execution_start_time_failing_because_execution_has_already_started(
    data_store: DataStore,
) -> None:
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        worker_status=WorkerStatus.RUNNING,
        worker_details=WORKER_DETAILS,
        termination_signal_sent=False,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=TIME,  # already started
        execution_finish_time=None,
    )

    with pytest.raises(ExecutionHasAlreadyStarted):
        data_store.executions.update(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            update_time=LATER_TIME,
            new_execution_start_time=LATER_TIME,
            should_already_have_started=False,  # requires that the execution has not yet started
        )

    execution = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
    )
    assert execution.execution_start_time == TIME  # unchanged


def test_update_execution_results(data_store: DataStore) -> None:
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        worker_status=WorkerStatus.RUNNING,
        worker_details=WORKER_DETAILS,
        termination_signal_sent=False,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=TIME,
        execution_finish_time=None,
    )

    data_store.executions.update(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        update_time=LATER_TIME,
        new_execution_finish_time=LATER_TIME,
        new_outcome=ExecutionOutcome.FAILED,
        new_output=OUTPUT_1,  # unrealistic for execution to have both output and error, but doesn't matter
        new_error_message=ERROR_MESSAGE,
        should_already_have_started=True,
        should_already_have_finished=False,
    )

    execution = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
    )
    assert execution.execution_finish_time == LATER_TIME
    assert execution.outcome == ExecutionOutcome.FAILED
    assert execution.output == OUTPUT_1
    assert execution.error_message == ERROR_MESSAGE

    all_executions = data_store.executions.list_all(
        worker_statuses={WorkerStatus.RUNNING}
    )
    assert len(all_executions) == 1
    assert all_executions[0].execution_finish_time == LATER_TIME
    assert all_executions[0].outcome == ExecutionOutcome.FAILED
    assert all_executions[0].output == OUTPUT_1
    assert all_executions[0].error_message == ERROR_MESSAGE

    executions_for_invocation = data_store.executions.list_for_invocation(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    ).executions
    assert len(executions_for_invocation) == 1
    assert executions_for_invocation[0].execution_finish_time == LATER_TIME
    assert executions_for_invocation[0].outcome == ExecutionOutcome.FAILED
    assert executions_for_invocation[0].output == OUTPUT_1
    assert executions_for_invocation[0].error_message == ERROR_MESSAGE

    invocation = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    )
    assert len(invocation.executions) == 1
    assert invocation.executions[0].execution_finish_time == LATER_TIME
    assert invocation.executions[0].outcome == ExecutionOutcome.FAILED
    assert invocation.executions[0].output == OUTPUT_1
    assert invocation.executions[0].error_message == ERROR_MESSAGE


def test_update_execution_results_failing_because_execution_not_yet_started(
    data_store: DataStore,
) -> None:
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        worker_status=WorkerStatus.PENDING,
        worker_details=None,
        termination_signal_sent=False,
        outcome=None,
        output=None,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=None,  # not yet started
        execution_finish_time=None,
    )

    with pytest.raises(ExecutionHasNotStarted):
        data_store.executions.update(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            update_time=LATER_TIME,
            new_execution_finish_time=LATER_TIME,
            new_outcome=ExecutionOutcome.SUCCEEDED,
            new_output=OUTPUT_1,
            should_already_have_started=True,  # requires that execution has already started
            should_already_have_finished=False,
        )

    execution = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
    )
    assert execution.outcome is None  # unchanged
    assert execution.output is None
    assert execution.execution_finish_time is None


def test_update_execution_results_failing_because_execution_already_finished(
    data_store: DataStore,
) -> None:
    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
        worker_status=WorkerStatus.RUNNING,
        worker_details=WORKER_DETAILS,
        termination_signal_sent=True,
        outcome=ExecutionOutcome.SUCCEEDED,
        output=OUTPUT_1,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=TIME,
        execution_finish_time=TIME,  # already finished
    )

    with pytest.raises(ExecutionHasAlreadyFinished):
        data_store.executions.update(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            update_time=LATER_TIME,
            new_execution_finish_time=LATER_TIME,
            new_outcome=ExecutionOutcome.ABORTED,
            should_already_have_started=True,
            should_already_have_finished=False,  # requires that execution has not already finished
        )

    execution = data_store.executions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        execution_id=EXECUTION_ID_1,
    )
    assert execution.outcome == ExecutionOutcome.SUCCEEDED  # unchanged
    assert execution.execution_finish_time == TIME


def test_update_when_execution_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(ExecutionDoesNotExist):
        data_store.executions.update(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            update_time=LATER_TIME,
            new_worker_status=WorkerStatus.RUNNING,
            new_worker_details=WORKER_DETAILS,
        )


def test_methods_when_invocation_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(InvocationDoesNotExist):
        data_store.executions.create(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=NONEXISTENT_INVOCATION_ID,
            execution_id=EXECUTION_ID_1,
            worker_status=WorkerStatus.PENDING,
            worker_details=None,
            termination_signal_sent=False,
            outcome=None,
            output=None,
            error_message=None,
            creation_time=TIME,
            last_update_time=TIME,
            execution_start_time=None,
            execution_finish_time=None,
        )

    with pytest.raises(InvocationDoesNotExist):
        data_store.executions.update(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=NONEXISTENT_INVOCATION_ID,
            execution_id=EXECUTION_ID_1,
            update_time=LATER_TIME,
            new_worker_status=WorkerStatus.RUNNING,
            new_worker_details=WORKER_DETAILS,
        )

    with pytest.raises(InvocationDoesNotExist):
        data_store.executions.list_for_invocation(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=NONEXISTENT_INVOCATION_ID,
        )


def test_methods_when_function_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(FunctionDoesNotExist):
        data_store.executions.create(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=NONEXISTENT_FUNCTION_NAME,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            worker_status=WorkerStatus.PENDING,
            worker_details=None,
            termination_signal_sent=False,
            outcome=None,
            output=None,
            error_message=None,
            creation_time=TIME,
            last_update_time=TIME,
            execution_start_time=None,
            execution_finish_time=None,
        )

    with pytest.raises(FunctionDoesNotExist):
        data_store.executions.update(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=NONEXISTENT_FUNCTION_NAME,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            update_time=LATER_TIME,
            new_worker_status=WorkerStatus.RUNNING,
            new_worker_details=WORKER_DETAILS,
        )

    with pytest.raises(FunctionDoesNotExist):
        data_store.executions.list_for_invocation(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=NONEXISTENT_FUNCTION_NAME,
            invocation_id=INVOCATION_ID_1,
        )


def test_methods_when_version_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(VersionDoesNotExist):
        data_store.executions.create(
            project_name=PROJECT_NAME,
            version_id=NONEXISTENT_VERSION_NAME,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            worker_status=WorkerStatus.PENDING,
            worker_details=None,
            termination_signal_sent=False,
            outcome=None,
            output=None,
            error_message=None,
            creation_time=TIME,
            last_update_time=TIME,
            execution_start_time=None,
            execution_finish_time=None,
        )

    with pytest.raises(VersionDoesNotExist):
        data_store.executions.update(
            project_name=PROJECT_NAME,
            version_id=NONEXISTENT_VERSION_NAME,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            update_time=LATER_TIME,
            new_worker_status=WorkerStatus.RUNNING,
            new_worker_details=WORKER_DETAILS,
        )

    with pytest.raises(VersionDoesNotExist):
        data_store.executions.list_for_invocation(
            project_name=PROJECT_NAME,
            version_id=NONEXISTENT_VERSION_NAME,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
        )


def test_methods_when_project_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(ProjectDoesNotExist):
        data_store.executions.create(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            worker_status=WorkerStatus.PENDING,
            worker_details=None,
            termination_signal_sent=False,
            outcome=None,
            output=None,
            error_message=None,
            creation_time=TIME,
            last_update_time=TIME,
            execution_start_time=None,
            execution_finish_time=None,
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.executions.update(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            execution_id=EXECUTION_ID_1,
            update_time=LATER_TIME,
            new_worker_status=WorkerStatus.RUNNING,
            new_worker_details=WORKER_DETAILS,
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.executions.list_for_invocation(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
        )
