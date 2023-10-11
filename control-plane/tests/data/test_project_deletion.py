from typing import Iterable

import pytest

from control_plane.data.data_store import DataStore
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import ProjectDoesNotExist
from control_plane.types.datatypes import (
    ExecutionOutcome,
    ExecutionSpec,
    FunctionStatus,
    InvocationStatus,
    ParentInvocationDefinition,
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


PROJECT_NAME = "project"
OTHER_PROJECT_NAME = "other-project"
VERSION_ID = "version"
FUNCTION_NAME = "function"
DOCKER_IMAGE = "image"
RESOURCE_SPEC = ResourceSpec(virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=5)
EXECUTION_SPEC = ExecutionSpec(timeout_seconds=300, max_retries=8)
PREPARED_FUNCTION_DETAILS = PreparedFunctionDetails(
    type=WorkerType.TEST, identifier="def"
)

INVOCATION_ID = "invocation"
PARENT_INVOCATION_ID = "parent-invocation"
INPUT = "input"
OUTPUT = "output"
EXECUTION_ID = "execution"
WORKER_DETAILS = WorkerDetails(
    type=WorkerType.TEST, identifier="worker", logs_identifier="logs"
)

TIME = 0


@pytest.fixture()
def data_store(conn_pool: SqlConnectionPool) -> Iterable[DataStore]:
    data_store = DataStore(conn_pool)
    data_store.create_tables()
    try:
        yield data_store
    finally:
        data_store.delete_tables()


def test_delete_with_cascade(data_store: DataStore) -> None:
    # Create a project with a full set of resources, to check that the cascading deletion works
    data_store.projects.create(
        project_name=PROJECT_NAME, deletion_request_time=TIME, creation_time=TIME
    )

    data_store.project_versions.create(
        project_name=PROJECT_NAME, version_id=VERSION_ID, creation_time=TIME
    )

    data_store.functions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        docker_image=DOCKER_IMAGE,
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=FunctionStatus.READY,
        prepared_function_details=PREPARED_FUNCTION_DETAILS,
    )

    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=PARENT_INVOCATION_ID,
        parent_invocation=None,
        input=INPUT,
        cancellation_request_time=None,
        invocation_status=InvocationStatus.TERMINATED,
        creation_time=TIME,
        last_update_time=TIME,
    )

    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        parent_invocation=ParentInvocationDefinition(
            function_name=FUNCTION_NAME, invocation_id=PARENT_INVOCATION_ID
        ),
        input=INPUT,
        cancellation_request_time=None,
        invocation_status=InvocationStatus.TERMINATED,
        creation_time=TIME,
        last_update_time=TIME,
    )

    data_store.executions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME,
        invocation_id=INVOCATION_ID,
        execution_id=EXECUTION_ID,
        worker_status=WorkerStatus.TERMINATED,
        worker_details=WORKER_DETAILS,
        termination_signal_time=None,
        outcome=ExecutionOutcome.SUCCEEDED,
        output=OUTPUT,
        error_message=None,
        creation_time=TIME,
        last_update_time=TIME,
        execution_start_time=TIME,
        execution_finish_time=TIME,
    )

    # Create another project, which should be left untouched by the deletion of the first project
    data_store.projects.create(
        project_name=OTHER_PROJECT_NAME, deletion_request_time=None, creation_time=TIME
    )

    # Delete the first project
    data_store.projects.delete_with_cascade(project_name=PROJECT_NAME)

    projects = data_store.projects.list().projects
    assert len(projects) == 1
    assert projects[0].project_name == OTHER_PROJECT_NAME

    assert len(data_store.functions.list_all(statuses={FunctionStatus.READY})) == 0
    assert (
        len(data_store.invocations.list_all(statuses={InvocationStatus.TERMINATED}))
        == 0
    )
    assert (
        len(data_store.executions.list_all(worker_statuses={WorkerStatus.TERMINATED}))
        == 0
    )


def test_delete_with_cascade_when_project_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(ProjectDoesNotExist):
        data_store.projects.delete_with_cascade(project_name=PROJECT_NAME)
