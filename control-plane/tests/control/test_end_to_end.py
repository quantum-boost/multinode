from typing import Iterable

import pytest

from control_plane.control.api.all import ApiHandler
from control_plane.control.periodic.all import LifecycleActions
from control_plane.data.data_store import DataStore
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.datatypes import (
    VersionDefinition,
    FunctionSpec,
    ResourceSpec,
    ExecutionSpec,
    InvocationDefinition,
    WorkerStatus,
    ExecutionTemporaryResultPayload,
    ExecutionFinalResultPayload,
    ExecutionOutcome,
    InvocationStatus,
    ParentInvocationDefinition,
)
from control_plane.control.utils.version_reference import (
    VersionReferenceType,
    VersionReference,
)
from tests.provisioning.dummy_provisioner import DummyProvisioner


@pytest.fixture(scope="module")
def conn_pool() -> Iterable[SqlConnectionPool]:
    conn_pool = SqlConnectionPool.create_for_local_postgres()
    try:
        yield conn_pool
    finally:
        conn_pool.close()


@pytest.fixture()
def data_store(conn_pool: SqlConnectionPool) -> Iterable[DataStore]:
    data_store = DataStore(conn_pool)
    data_store.create_tables()
    try:
        yield data_store
    finally:
        data_store.delete_tables()


PROJECT_NAME = "project"
LATEST_VERSION = VersionReference(
    type=VersionReferenceType.LATEST, named_version_id=None
)

STANDARD_FUNCTION = "standard-function"  # max concurrency = 100, max_retries = 0
CONCURRENCY_LIMITED_FUNCTION = (
    "concurrency-limit-function"  # max concurrency = 1, max retries = 0
)
RETRYABLE_FUNCTION = "retryable-function"  # max concurrency = 100, max retries = 5

INPUT_1 = "input-1"
INPUT_2 = "input-2"
TEMPORARY_OUTPUT = "temp-output"
FINAL_OUTPUT = "final-output"
ERROR_MESSAGE = "error"

TIMEOUT_SECONDS = 30

# Won't bother incrementing the time in tests, since the control mechanism doesn't rely on time.
# The only exception is timeouts, where time is crucial.
TIME = 0

VERSION_DEFINITION = VersionDefinition(
    default_docker_image="docker",
    functions=[
        FunctionSpec(
            function_name=STANDARD_FUNCTION,
            resource_spec=ResourceSpec(
                virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=100
            ),
            execution_spec=ExecutionSpec(
                timeout_seconds=TIMEOUT_SECONDS, max_retries=0
            ),
        ),
        FunctionSpec(
            function_name=CONCURRENCY_LIMITED_FUNCTION,
            resource_spec=ResourceSpec(
                virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=1
            ),
            execution_spec=ExecutionSpec(
                timeout_seconds=TIMEOUT_SECONDS, max_retries=0
            ),
        ),
        FunctionSpec(
            function_name=RETRYABLE_FUNCTION,
            resource_spec=ResourceSpec(
                virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=100
            ),
            execution_spec=ExecutionSpec(
                timeout_seconds=TIMEOUT_SECONDS, max_retries=5
            ),
        ),
    ],
)


@pytest.mark.timeout(5)
def test_two_invocations_running_in_parallel_with_one_succeeding_and_one_failing(
    data_store: DataStore,
) -> None:
    provisioner = DummyProvisioner()

    api = ApiHandler(data_store, provisioner)
    loop = LifecycleActions(data_store, provisioner)

    api.registration.create_project(project_name=PROJECT_NAME, time=TIME)
    api.registration.create_project_version(
        project_name=PROJECT_NAME, version_definition=VERSION_DEFINITION, time=TIME
    )

    # The invoker creates two invocations of the function that can run multiple workers concurrently.
    invocation_1 = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_1),
        time=TIME,
    )
    invocation_1_id = invocation_1.invocation_id

    invocation_2 = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_2),
        time=TIME,
    )
    invocation_2_id = invocation_2.invocation_id

    # Wait till both invocations have an execution with a worker in RUNNING status.
    while not any(
        execution.worker_status == WorkerStatus.RUNNING
        for execution in api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=invocation_1_id,
        ).executions
    ):
        loop.run_once(TIME)

    while not any(
        execution.worker_status == WorkerStatus.RUNNING
        for execution in api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=invocation_2_id,
        ).executions
    ):
        loop.run_once(TIME)

    invocation_1 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_1_id,
    )
    assert invocation_1.executions[0].execution_start_time is None
    assert invocation_1.executions[0].execution_finish_time is None

    invocation_2 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_2_id,
    )
    assert invocation_2.executions[0].execution_start_time is None
    assert invocation_2.executions[0].execution_finish_time is None

    execution_1_id = invocation_1.executions[0].execution_id
    execution_2_id = invocation_2.executions[0].execution_id

    execution_1 = api.execution.get_execution(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_1_id,
        execution_id=execution_1_id,
    )
    execution_2 = api.execution.get_execution(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_2_id,
        execution_id=execution_2_id,
    )

    assert execution_1.worker_details is not None
    assert execution_2.worker_details is not None

    assert provisioner.worker_is_provisioned(execution_1.worker_details)
    assert provisioner.worker_is_provisioned(execution_2.worker_details)

    # The two workers should now be able to get instructions about the computation they have to perform.
    assert execution_1.input == INPUT_1
    assert not execution_1.cancellation_requested

    assert execution_2.input == INPUT_2
    assert not execution_2.cancellation_requested

    # The two workers signal that they are starting their executions.
    api.execution.mark_execution_as_started(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_1_id,
        execution_id=execution_1_id,
        time=TIME,
    )
    api.execution.mark_execution_as_started(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_2_id,
        execution_id=execution_2_id,
        time=TIME,
    )

    # The invoker should be able to see that the workers have started their executions
    invocation_1 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_1_id,
    )
    assert invocation_1.executions[0].execution_start_time == TIME
    assert invocation_1.executions[0].execution_finish_time is None

    invocation_2 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_2_id,
    )
    assert invocation_2.executions[0].execution_start_time == TIME
    assert invocation_2.executions[0].execution_finish_time is None

    # The first worker supplies a temporary output (i.e. a progress update).
    api.execution.upload_temporary_execution_result(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_1_id,
        execution_id=execution_1_id,
        temporary_result_payload=ExecutionTemporaryResultPayload(
            latest_output=TEMPORARY_OUTPUT
        ),
        time=TIME,
    )

    # The invoker should be able to see this temporary output.
    invocation_1 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_1_id,
    )
    assert invocation_1.executions[0].outcome is None
    assert invocation_1.executions[0].output == TEMPORARY_OUTPUT
    assert invocation_1.executions[0].execution_start_time == TIME
    assert invocation_1.executions[0].execution_finish_time is None

    # The first worker supplies the final output.
    api.execution.set_final_execution_result(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_1_id,
        execution_id=execution_1_id,
        final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.SUCCEEDED, final_output=FINAL_OUTPUT
        ),
        time=TIME,
    )

    # The invoker should be able to see this SUCCEEDED status and this final output.
    invocation_1 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_1_id,
    )
    assert invocation_1.executions[0].outcome == ExecutionOutcome.SUCCEEDED
    assert invocation_1.executions[0].output == FINAL_OUTPUT
    assert invocation_1.executions[0].execution_start_time == TIME
    assert invocation_1.executions[0].execution_finish_time == TIME

    # The second worker throws an error.
    api.execution.set_final_execution_result(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_2_id,
        execution_id=execution_2_id,
        final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.FAILED, error_message=ERROR_MESSAGE
        ),
        time=TIME,
    )

    # The invoker should be able to see this FAILED status and this error message
    invocation_2 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_2_id,
    )
    assert invocation_2.executions[0].outcome == ExecutionOutcome.FAILED
    assert invocation_2.executions[0].error_message == ERROR_MESSAGE
    assert invocation_2.executions[0].execution_start_time == TIME
    assert invocation_2.executions[0].execution_finish_time == TIME

    # Having finished their executions, the two workers terminate.
    provisioner.mock_worker_termination(execution_1.worker_details)
    provisioner.mock_worker_termination(execution_2.worker_details)

    # Note that this function has max_retries = 0, so the invocation with the failed execution is not retried.

    # Wait till both invocations are terminated
    while (
        not api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=invocation_1_id,
        ).invocation_status
        == InvocationStatus.TERMINATED
    ):
        loop.run_once(TIME)

    while (
        not api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=invocation_2_id,
        ).invocation_status
        == InvocationStatus.TERMINATED
    ):
        loop.run_once(TIME)

    invocation_1 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_1_id,
    )
    assert len(invocation_1.executions) == 1
    assert invocation_1.executions[0].worker_status == WorkerStatus.TERMINATED

    invocation_2 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_1_id,
    )
    assert len(invocation_2.executions) == 1
    assert invocation_2.executions[0].worker_status == WorkerStatus.TERMINATED

    # The logs from the executions should be accessible
    logs = api.logs.get_execution_logs(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_2_id,
        execution_id=execution_2_id,
        max_lines=None,
        initial_offset=None,
    )
    assert isinstance(logs.log_lines, list)


@pytest.mark.timeout(5)
def test_parent_and_child_invocations_with_parent_cancelled_while_both_executions_in_flight(
    data_store: DataStore,
) -> None:
    provisioner = DummyProvisioner()

    api = ApiHandler(data_store, provisioner)
    loop = LifecycleActions(data_store, provisioner)

    api.registration.create_project(project_name=PROJECT_NAME, time=TIME)
    api.registration.create_project_version(
        project_name=PROJECT_NAME, version_definition=VERSION_DEFINITION, time=TIME
    )

    # The parent invocation is created.
    parent_invocation = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_1),
        time=TIME,
    )
    parent_invocation_id = parent_invocation.invocation_id

    # Wait till the parent invocation has an execution in RUNNING status.
    while not any(
        execution.worker_status == WorkerStatus.RUNNING
        for execution in api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=parent_invocation_id,
        ).executions
    ):
        loop.run_once(TIME)

    parent_invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=parent_invocation_id,
    )
    parent_execution_id = parent_invocation.executions[0].execution_id
    parent_execution = api.execution.get_execution(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=parent_invocation_id,
        execution_id=parent_execution_id,
    )

    assert parent_execution.input == INPUT_1
    assert parent_execution.worker_details is not None

    # The worker starts the parent execution.
    api.execution.mark_execution_as_started(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=parent_invocation_id,
        execution_id=parent_execution_id,
        time=TIME,
    )

    # This worker creates a child invocation.
    # This child invocation can execute in parallel with the parent invocation.
    child_invocation = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_definition=InvocationDefinition(
            input=INPUT_2,
            parent_invocation=ParentInvocationDefinition(
                function_name=STANDARD_FUNCTION, invocation_id=parent_invocation_id
            ),
        ),
        time=TIME,
    )
    child_invocation_id = child_invocation.invocation_id

    # Wait till the child invocation has an execution in RUNNING status.
    while not any(
        execution.worker_status == WorkerStatus.RUNNING
        for execution in api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=child_invocation_id,
        ).executions
    ):
        loop.run_once(TIME)

    child_invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=child_invocation_id,
    )
    child_execution_id = child_invocation.executions[0].execution_id
    child_execution = api.execution.get_execution(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=child_invocation_id,
        execution_id=child_execution_id,
    )

    assert child_execution.input == INPUT_2
    assert child_execution.worker_details is not None

    # The worker starts the child execution.
    api.execution.mark_execution_as_started(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=child_invocation_id,
        execution_id=child_execution_id,
        time=TIME,
    )

    # Because no cancellation requests have been sent, the workers will not receive a termination request.
    for _ in range(10):
        loop.run_once(TIME)

    assert not provisioner.worker_has_received_termination_signal(
        parent_execution.worker_details
    )
    assert not provisioner.worker_has_received_termination_signal(
        child_execution.worker_details
    )

    # The invoker sends a cancellation request for the parent invocation.
    api.invocation.cancel_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=parent_invocation_id,
        time=TIME,
    )

    # Wait until the worker for the parent invocation receives a termination signal.
    while not provisioner.worker_has_received_termination_signal(
        parent_execution.worker_details
    ):
        loop.run_once(TIME)

    # As some point, the termination request should be propagated from the parent to the child.
    while not api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=child_invocation_id,
    ).cancellation_requested:
        loop.run_once(TIME)

    # Wait until the worker for the child invocation also receives a termination signal.
    while not provisioner.worker_has_received_termination_signal(
        child_execution.worker_details
    ):
        loop.run_once(TIME)

    # Both workers clean up gracefully and terminate.
    api.execution.set_final_execution_result(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=parent_invocation_id,
        execution_id=parent_execution_id,
        final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.ABORTED
        ),
        time=TIME,
    )
    provisioner.mock_worker_termination(parent_execution.worker_details)

    api.execution.set_final_execution_result(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=child_invocation_id,
        execution_id=child_execution_id,
        final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.ABORTED
        ),
        time=TIME,
    )
    provisioner.mock_worker_termination(child_execution.worker_details)

    # The ABORTED outcome should be visible to the invoker.
    parent_invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=parent_invocation_id,
    )
    assert parent_invocation.executions[0].outcome == ExecutionOutcome.ABORTED

    # Eventually, both invocations are reach TERMINATED status
    while (
        not api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=parent_invocation_id,
        ).invocation_status
        == InvocationStatus.TERMINATED
    ):
        loop.run_once(TIME)

    while (
        not api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=child_invocation_id,
        ).invocation_status
        == InvocationStatus.TERMINATED
    ):
        loop.run_once(TIME)


@pytest.mark.timeout(5)
def test_invocation_timing_out_while_execution_in_flight(data_store: DataStore) -> None:
    provisioner = DummyProvisioner()

    api = ApiHandler(data_store, provisioner)
    loop = LifecycleActions(data_store, provisioner)

    api.registration.create_project(project_name=PROJECT_NAME, time=TIME)
    api.registration.create_project_version(
        project_name=PROJECT_NAME, version_definition=VERSION_DEFINITION, time=TIME
    )

    # The invocation is created.
    invocation = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_1),
        time=TIME,
    )
    invocation_id = invocation.invocation_id

    # Wait till the invocation has an execution in RUNNING status.
    while not any(
        execution.worker_status == WorkerStatus.RUNNING
        for execution in api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=invocation_id,
        ).executions
    ):
        loop.run_once(TIME)

    invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_id,
    )
    execution_id = invocation.executions[0].execution_id
    execution = api.execution.get_execution(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id,
    )

    assert execution.worker_details is not None

    # The worker starts the execution.
    api.execution.mark_execution_as_started(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id,
        time=TIME,
    )

    # But the worker takes a long time to execute.

    # Meanwhile, the clock is ticking. Eventually, the invocation times out, resulting in a termination signal
    # being sent to the worker.
    time = TIME
    while not provisioner.worker_has_received_termination_signal(
        execution.worker_details
    ):
        loop.run_once(time)
        time += 1

    assert time >= TIME + TIMEOUT_SECONDS

    # The worker gracefully cleans up and terminates.
    api.execution.set_final_execution_result(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id,
        final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.ABORTED
        ),
        time=TIME,
    )
    provisioner.mock_worker_termination(execution.worker_details)

    # The ABORTED outcome should be visible to the invoker.
    invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_id,
    )
    assert invocation.executions[0].outcome == ExecutionOutcome.ABORTED

    # Eventually, the invocation reaches TERMINATED status
    while (
        not api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=invocation_id,
        ).invocation_status
        == InvocationStatus.TERMINATED
    ):
        loop.run_once(TIME)


@pytest.mark.timeout(5)
def test_invocation_timing_out_while_execution_in_flight_and_cleanup_not_finishing_within_grace_period(
    data_store: DataStore,
) -> None:
    provisioner = DummyProvisioner()

    api = ApiHandler(data_store, provisioner)
    loop = LifecycleActions(data_store, provisioner)

    api.registration.create_project(project_name=PROJECT_NAME, time=TIME)
    api.registration.create_project_version(
        project_name=PROJECT_NAME, version_definition=VERSION_DEFINITION, time=TIME
    )

    # The invocation is created.
    invocation = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_1),
        time=TIME,
    )
    invocation_id = invocation.invocation_id

    # Wait till the invocation has an execution in RUNNING status.
    while not any(
        execution.worker_status == WorkerStatus.RUNNING
        for execution in api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=invocation_id,
        ).executions
    ):
        loop.run_once(TIME)

    invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_id,
    )
    execution_id = invocation.executions[0].execution_id
    execution = api.execution.get_execution(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id,
    )

    assert execution.worker_details is not None

    # The worker starts the execution.
    api.execution.mark_execution_as_started(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id,
        time=TIME,
    )

    # The invocation times out, resulting in a termination signal being sent to the worker.
    time = TIME
    while not provisioner.worker_has_received_termination_signal(
        execution.worker_details
    ):
        loop.run_once(time)
        time += 1

    # The worker tries to gracefully clean up, but the clean-up itself takes a long time.
    # Before the clean-up finishes, the grace period elapsed, so the worker is forcibly killed.
    provisioner.mock_worker_termination(execution.worker_details)

    # Eventually, the invocation reaches TERMINATED status
    while (
        not api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=STANDARD_FUNCTION,
            invocation_id=invocation_id,
        ).invocation_status
        == InvocationStatus.TERMINATED
    ):
        loop.run_once(TIME)

    # However, the execution does not reach any outcome (not even an ABORTED outcome).
    invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=STANDARD_FUNCTION,
        invocation_id=invocation_id,
    )
    assert invocation.executions[0].outcome is None


@pytest.mark.timeout(5)
def test_invocation_being_cancelled_before_worker_is_provisioned(
    data_store: DataStore,
) -> None:
    provisioner = DummyProvisioner()

    api = ApiHandler(data_store, provisioner)
    loop = LifecycleActions(data_store, provisioner)

    api.registration.create_project(project_name=PROJECT_NAME, time=TIME)
    api.registration.create_project_version(
        project_name=PROJECT_NAME, version_definition=VERSION_DEFINITION, time=TIME
    )

    # An invocation is created. The function we're using has a concurrency limit of 1.
    invocation_1 = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=CONCURRENCY_LIMITED_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_1),
        time=TIME,
    )
    invocation_id_1 = invocation_1.invocation_id

    # An execution is created for this invocation
    while (
        not len(
            api.invocation.get_invocation(
                project_name=PROJECT_NAME,
                version_ref=LATEST_VERSION,
                function_name=CONCURRENCY_LIMITED_FUNCTION,
                invocation_id=invocation_id_1,
            ).executions
        )
        == 1
    ):
        loop.run_once(TIME)

    # A second invocation is created for the same function. But since the concurrency limit for this function is 1,
    # no execution will be created for this second invocation.
    invocation_2 = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=CONCURRENCY_LIMITED_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_2),
        time=TIME,
    )
    invocation_id_2 = invocation_2.invocation_id

    for _ in range(10):
        loop.run_once(TIME)

    invocation_2 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=CONCURRENCY_LIMITED_FUNCTION,
        invocation_id=invocation_id_2,
    )
    assert len(invocation_2.executions) == 0
    assert invocation_2.invocation_status == InvocationStatus.RUNNING

    # The second invocation receives a cancellation request.
    api.invocation.cancel_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=CONCURRENCY_LIMITED_FUNCTION,
        invocation_id=invocation_id_2,
        time=TIME,
    )

    # As a result, the second invocation reaches TERMINATED status, without an execution being created.
    while (
        not api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=CONCURRENCY_LIMITED_FUNCTION,
            invocation_id=invocation_id_2,
        ).invocation_status
        == InvocationStatus.TERMINATED
    ):
        loop.run_once(TIME)

    invocation_2 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=CONCURRENCY_LIMITED_FUNCTION,
        invocation_id=invocation_id_2,
    )
    assert invocation_2.invocation_status == InvocationStatus.TERMINATED
    assert len(invocation_2.executions) == 0

    # Meanwhile, invocation 1 stays in RUNNING status
    invocation_1 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=CONCURRENCY_LIMITED_FUNCTION,
        invocation_id=invocation_id_1,
    )
    assert invocation_1.invocation_status == InvocationStatus.RUNNING


@pytest.mark.timeout(5)
def test_invocation_timing_out_before_worker_is_provisioned(
    data_store: DataStore,
) -> None:
    provisioner = DummyProvisioner()

    api = ApiHandler(data_store, provisioner)
    loop = LifecycleActions(data_store, provisioner)

    api.registration.create_project(project_name=PROJECT_NAME, time=TIME)
    api.registration.create_project_version(
        project_name=PROJECT_NAME, version_definition=VERSION_DEFINITION, time=TIME
    )

    # An invocation is created. The function we're using has a concurrency limit of 1.
    invocation_1 = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=CONCURRENCY_LIMITED_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_1),
        time=TIME,
    )
    invocation_id_1 = invocation_1.invocation_id

    # An execution is created for this invocation
    while (
        not len(
            api.invocation.get_invocation(
                project_name=PROJECT_NAME,
                version_ref=LATEST_VERSION,
                function_name=CONCURRENCY_LIMITED_FUNCTION,
                invocation_id=invocation_id_1,
            ).executions
        )
        == 1
    ):
        loop.run_once(TIME)

    # A second invocation is created for the same function. But since the concurrency limit for this function is 1,
    # no execution will be created for this second invocation.
    invocation_2 = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=CONCURRENCY_LIMITED_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_2),
        time=TIME,
    )
    invocation_id_2 = invocation_2.invocation_id

    for _ in range(10):
        loop.run_once(TIME)

    invocation_2 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=CONCURRENCY_LIMITED_FUNCTION,
        invocation_id=invocation_id_2,
    )
    assert len(invocation_2.executions) == 0
    assert invocation_2.invocation_status == InvocationStatus.RUNNING

    # Eventually, the second invocation times out. It goes into TERMINATED status, without an execution being created.
    time = TIME
    while (
        not api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=CONCURRENCY_LIMITED_FUNCTION,
            invocation_id=invocation_id_2,
        ).invocation_status
        == InvocationStatus.TERMINATED
    ):
        loop.run_once(time)
        time += 1

    invocation_2 = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=CONCURRENCY_LIMITED_FUNCTION,
        invocation_id=invocation_id_2,
    )
    assert invocation_2.invocation_status == InvocationStatus.TERMINATED
    assert len(invocation_2.executions) == 0
    assert time >= TIME + TIMEOUT_SECONDS


@pytest.mark.timeout(5)
def test_invocation_being_retried_due_to_error_being_thrown_in_code(
    data_store: DataStore,
) -> None:
    provisioner = DummyProvisioner()

    api = ApiHandler(data_store, provisioner)
    loop = LifecycleActions(data_store, provisioner)

    api.registration.create_project(project_name=PROJECT_NAME, time=TIME)
    api.registration.create_project_version(
        project_name=PROJECT_NAME, version_definition=VERSION_DEFINITION, time=TIME
    )

    # An invocation is created. The function we're using allows for retries.
    invocation = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_1),
        time=TIME,
    )
    invocation_id = invocation.invocation_id

    # Wait till the invocation has an execution in RUNNING status.
    while not any(
        execution.worker_status == WorkerStatus.RUNNING
        for execution in api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=RETRYABLE_FUNCTION,
            invocation_id=invocation_id,
        ).executions
    ):
        loop.run_once(TIME)

    invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
    )
    execution_id_1 = invocation.executions[0].execution_id
    execution_1 = api.execution.get_execution(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_1,
    )

    assert execution_1.worker_details is not None

    # The worker signals that it has started execution
    api.execution.mark_execution_as_started(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_1,
        time=TIME,
    )

    # The worker throws an error and terminates
    api.execution.set_final_execution_result(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_1,
        final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.FAILED, error_message=ERROR_MESSAGE
        ),
        time=TIME,
    )
    provisioner.mock_worker_termination(execution_1.worker_details)

    # At some point, a second execution will be created for this invocation.
    while (
        not len(
            api.invocation.get_invocation(
                project_name=PROJECT_NAME,
                version_ref=LATEST_VERSION,
                function_name=RETRYABLE_FUNCTION,
                invocation_id=invocation_id,
            ).executions
        )
        == 2
    ):
        loop.run_once(TIME)

    invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
    )
    executions_not_in_terminated_status = [
        execution
        for execution in invocation.executions
        if execution.worker_status != WorkerStatus.TERMINATED
    ]
    assert len(executions_not_in_terminated_status) == 1
    execution_id_2 = executions_not_in_terminated_status[0].execution_id

    # Wait till the second execution reaches RUNNING status.
    while not any(
        execution.worker_status == WorkerStatus.RUNNING
        and execution.execution_id == execution_id_2
        for execution in api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=RETRYABLE_FUNCTION,
            invocation_id=invocation_id,
        ).executions
    ):
        loop.run_once(TIME)

    execution_2 = api.execution.get_execution(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_2,
    )

    assert execution_2.worker_details is not None

    # The second worker signals that it has started execution.
    api.execution.mark_execution_as_started(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_2,
        time=TIME,
    )

    # The second worker completes successfully and terminates.
    api.execution.set_final_execution_result(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_2,
        final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.SUCCEEDED, final_output=FINAL_OUTPUT
        ),
        time=TIME,
    )
    provisioner.mock_worker_termination(execution_2.worker_details)

    # Wait for the invocation to reach TERMINATED status
    while (
        not api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=RETRYABLE_FUNCTION,
            invocation_id=invocation_id,
        ).invocation_status
        == InvocationStatus.TERMINATED
    ):
        loop.run_once(TIME)

    # Both executions should be visible to the invoker.
    # The first execution should have a FAILED outcome. The second execution should have a SUCCEEDED outcome.
    invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
    )
    assert len(invocation.executions) == 2
    assert all(
        execution.worker_status == WorkerStatus.TERMINATED
        for execution in invocation.executions
    )
    assert any(
        execution.outcome == ExecutionOutcome.FAILED
        for execution in invocation.executions
    )
    assert any(
        execution.outcome == ExecutionOutcome.SUCCEEDED
        for execution in invocation.executions
    )


@pytest.mark.timeout(5)
def test_invocation_being_retried_due_to_hardware_failure(
    data_store: DataStore,
) -> None:
    provisioner = DummyProvisioner()

    api = ApiHandler(data_store, provisioner)
    loop = LifecycleActions(data_store, provisioner)

    api.registration.create_project(project_name=PROJECT_NAME, time=TIME)
    api.registration.create_project_version(
        project_name=PROJECT_NAME, version_definition=VERSION_DEFINITION, time=TIME
    )

    # An invocation is created. The function we're using allows for retries.
    invocation = api.invocation.create_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_definition=InvocationDefinition(input=INPUT_1),
        time=TIME,
    )
    invocation_id = invocation.invocation_id

    # Wait till the invocation has an execution in RUNNING status.
    while not any(
        execution.worker_status == WorkerStatus.RUNNING
        for execution in api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=RETRYABLE_FUNCTION,
            invocation_id=invocation_id,
        ).executions
    ):
        loop.run_once(TIME)

    invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
    )
    execution_id_1 = invocation.executions[0].execution_id
    execution_1 = api.execution.get_execution(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_1,
    )

    assert execution_1.worker_details is not None

    # The worker signals that it has started execution
    api.execution.mark_execution_as_started(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_1,
        time=TIME,
    )

    # The worker suffers a hardware failure, and terminates abruptly.
    provisioner.mock_worker_termination(execution_1.worker_details)

    # A second execution will be created for this invocation.
    while (
        not len(
            api.invocation.get_invocation(
                project_name=PROJECT_NAME,
                version_ref=LATEST_VERSION,
                function_name=RETRYABLE_FUNCTION,
                invocation_id=invocation_id,
            ).executions
        )
        == 2
    ):
        loop.run_once(TIME)

    invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
    )
    executions_not_in_terminated_status = [
        execution
        for execution in invocation.executions
        if execution.worker_status != WorkerStatus.TERMINATED
    ]
    execution_id_2 = executions_not_in_terminated_status[0].execution_id

    # Wait till the second execution reaches RUNNING status.
    while not any(
        execution.worker_status == WorkerStatus.RUNNING
        and execution.execution_id == execution_id_2
        for execution in api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=RETRYABLE_FUNCTION,
            invocation_id=invocation_id,
        ).executions
    ):
        loop.run_once(TIME)

    execution_2 = api.execution.get_execution(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_2,
    )

    assert execution_2.worker_details is not None

    # The second worker signals that it has started execution.
    api.execution.mark_execution_as_started(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_2,
        time=TIME,
    )

    # The second worker completes successfully and terminates.
    api.execution.set_final_execution_result(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
        execution_id=execution_id_2,
        final_result_payload=ExecutionFinalResultPayload(
            outcome=ExecutionOutcome.SUCCEEDED, final_output=FINAL_OUTPUT
        ),
        time=TIME,
    )
    provisioner.mock_worker_termination(execution_2.worker_details)

    # Wait for the invocation to reach TERMINATED status
    while (
        not api.invocation.get_invocation(
            project_name=PROJECT_NAME,
            version_ref=LATEST_VERSION,
            function_name=RETRYABLE_FUNCTION,
            invocation_id=invocation_id,
        ).invocation_status
        == InvocationStatus.TERMINATED
    ):
        loop.run_once(TIME)

    # Both executions should be visible to the invoker.
    # The first execution should have no outcome; the second execution should have a SUCCEEDED outcome
    invocation = api.invocation.get_invocation(
        project_name=PROJECT_NAME,
        version_ref=LATEST_VERSION,
        function_name=RETRYABLE_FUNCTION,
        invocation_id=invocation_id,
    )
    assert len(invocation.executions) == 2
    assert all(
        execution.worker_status == WorkerStatus.TERMINATED
        for execution in invocation.executions
    )
    assert any(execution.outcome is None for execution in invocation.executions)
    assert any(
        execution.outcome == ExecutionOutcome.SUCCEEDED
        for execution in invocation.executions
    )
