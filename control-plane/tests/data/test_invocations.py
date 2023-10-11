from typing import Iterable

import pytest

from control_plane.data.data_store import DataStore
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import (
    FunctionDoesNotExist,
    InvocationAlreadyExists,
    InvocationDoesNotExist,
    ParentInvocationDoesNotExist,
    ProjectDoesNotExist,
    VersionDoesNotExist,
)
from control_plane.types.datatypes import (
    ExecutionSpec,
    FunctionStatus,
    InvocationStatus,
    ParentInvocationDefinition,
    PreparedFunctionDetails,
    ResourceSpec,
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

DOCKER_IMAGE_FOR_FUNCTION_1 = "image-1"
RESOURCE_SPEC_FOR_FUNCTION_1 = ResourceSpec(
    virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=5
)
EXECUTION_SPEC_FOR_FUNCTION_1 = ExecutionSpec(timeout_seconds=300, max_retries=8)
PREPARED_DETAILS_FOR_FUNCTION_1 = PreparedFunctionDetails(
    type=WorkerType.TEST, identifier="def-1"
)

DOCKER_IMAGE_FOR_FUNCTION_2 = "image-2"
RESOURCE_SPEC_FOR_FUNCTION_2 = ResourceSpec(
    virtual_cpus=8.0, memory_gbs=16.0, max_concurrency=20
)
EXECUTION_SPEC_FOR_FUNCTION_2 = ExecutionSpec(timeout_seconds=600, max_retries=19)

INVOCATION_ID_1 = "invocation-1"
INVOCATION_ID_2 = "invocation-2"

INPUT_1 = "input-1"

INPUT_2 = "input-2"

TIME = 0
LATER_TIME = 10


@pytest.fixture()
def data_store(conn_pool: SqlConnectionPool) -> Iterable[DataStore]:
    data_store = DataStore(conn_pool)
    data_store.create_tables()

    # Set up each test with the project, version and functions already inserted.
    data_store.projects.create(
        project_name=PROJECT_NAME, deletion_request_time=None, creation_time=TIME
    )

    data_store.project_versions.create(
        project_name=PROJECT_NAME, version_id=VERSION_ID, creation_time=TIME
    )

    data_store.functions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        docker_image=DOCKER_IMAGE_FOR_FUNCTION_1,
        resource_spec=RESOURCE_SPEC_FOR_FUNCTION_1,
        execution_spec=EXECUTION_SPEC_FOR_FUNCTION_1,
        function_status=FunctionStatus.READY,
        prepared_function_details=PREPARED_DETAILS_FOR_FUNCTION_1,
    )
    data_store.functions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        docker_image=DOCKER_IMAGE_FOR_FUNCTION_2,
        resource_spec=RESOURCE_SPEC_FOR_FUNCTION_2,
        execution_spec=EXECUTION_SPEC_FOR_FUNCTION_2,
        function_status=FunctionStatus.PENDING,
        prepared_function_details=None,
    )

    try:
        yield data_store

    finally:
        data_store.delete_tables()


def test_create_two_invocations_for_different_functions(data_store: DataStore) -> None:
    # To begin with, no invocations have been created.
    with pytest.raises(InvocationDoesNotExist):
        data_store.invocations.get(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
        )

    all_invocations = data_store.invocations.list_all(
        statuses={InvocationStatus.RUNNING}
    )
    assert len(all_invocations) == 0

    invocations_for_function = data_store.invocations.list_for_function(
        project_name=PROJECT_NAME, version_id=VERSION_ID, function_name=FUNCTION_NAME_1
    ).invocations
    assert len(invocations_for_function) == 0

    # Create first invocation
    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        parent_invocation=None,
        input=INPUT_1,
        cancellation_requested=False,
        invocation_status=InvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME,
    )

    invocation_1 = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    )

    assert invocation_1.project_name == PROJECT_NAME
    assert invocation_1.version_id == VERSION_ID
    assert invocation_1.function_name == FUNCTION_NAME_1
    assert invocation_1.invocation_id == INVOCATION_ID_1
    assert invocation_1.parent_invocation is None
    assert invocation_1.resource_spec.virtual_cpus == pytest.approx(
        RESOURCE_SPEC_FOR_FUNCTION_1.virtual_cpus
    )
    assert invocation_1.execution_spec == EXECUTION_SPEC_FOR_FUNCTION_1
    assert invocation_1.function_status == FunctionStatus.READY
    assert invocation_1.prepared_function_details == PREPARED_DETAILS_FOR_FUNCTION_1
    assert invocation_1.input == INPUT_1
    assert invocation_1.cancellation_requested == False
    assert invocation_1.invocation_status == InvocationStatus.RUNNING
    assert invocation_1.creation_time == TIME
    assert invocation_1.last_update_time == TIME

    all_invocations = data_store.invocations.list_all(
        statuses={InvocationStatus.RUNNING}
    )
    assert len(all_invocations) == 1
    assert all_invocations[0].project_name == PROJECT_NAME
    assert all_invocations[0].version_id == VERSION_ID
    assert all_invocations[0].function_name == FUNCTION_NAME_1
    assert all_invocations[0].invocation_id == INVOCATION_ID_1
    assert all_invocations[0].parent_invocation is None
    assert all_invocations[0].resource_spec.virtual_cpus == pytest.approx(
        RESOURCE_SPEC_FOR_FUNCTION_1.virtual_cpus
    )
    assert all_invocations[0].execution_spec == EXECUTION_SPEC_FOR_FUNCTION_1
    assert all_invocations[0].function_status == FunctionStatus.READY
    assert (
        all_invocations[0].prepared_function_details == PREPARED_DETAILS_FOR_FUNCTION_1
    )
    assert all_invocations[0].input == INPUT_1
    assert all_invocations[0].cancellation_requested == False
    assert all_invocations[0].invocation_status == InvocationStatus.RUNNING
    assert all_invocations[0].creation_time == TIME
    assert all_invocations[0].last_update_time == TIME

    invocations_for_function = data_store.invocations.list_for_function(
        project_name=PROJECT_NAME, version_id=VERSION_ID, function_name=FUNCTION_NAME_1
    ).invocations
    assert len(invocations_for_function) == 1
    assert invocations_for_function[0].invocation_id == INVOCATION_ID_1
    assert invocations_for_function[0].parent_invocation is None
    assert invocations_for_function[0].cancellation_requested == False
    assert invocations_for_function[0].invocation_status == InvocationStatus.RUNNING
    assert invocations_for_function[0].creation_time == TIME
    assert invocations_for_function[0].last_update_time == TIME

    invocations_for_other_function = data_store.invocations.list_for_function(
        project_name=PROJECT_NAME, version_id=VERSION_ID, function_name=FUNCTION_NAME_2
    ).invocations
    assert len(invocations_for_other_function) == 0

    # Create second invocation, which will be associated with the other function
    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        invocation_id=INVOCATION_ID_2,
        parent_invocation=None,
        input=INPUT_2,
        cancellation_requested=False,
        invocation_status=InvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME,
    )

    invocation_1 = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    )
    assert invocation_1.invocation_id == INVOCATION_ID_1

    invocation_2 = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        invocation_id=INVOCATION_ID_2,
    )
    assert invocation_2.invocation_id == INVOCATION_ID_2
    assert invocation_2.function_name == FUNCTION_NAME_2
    assert invocation_2.resource_spec.virtual_cpus == pytest.approx(
        RESOURCE_SPEC_FOR_FUNCTION_2.virtual_cpus
    )
    assert invocation_2.execution_spec == EXECUTION_SPEC_FOR_FUNCTION_2
    assert invocation_2.function_status == FunctionStatus.PENDING
    assert invocation_2.prepared_function_details is None

    all_invocations = data_store.invocations.list_all(
        statuses={InvocationStatus.RUNNING}
    )
    assert len(all_invocations) == 2

    invocations_for_function_2 = data_store.invocations.list_for_function(
        project_name=PROJECT_NAME, version_id=VERSION_ID, function_name=FUNCTION_NAME_2
    ).invocations
    assert len(invocations_for_function_2) == 1

    # Mismatch between function name and invocation id => should get error
    with pytest.raises(InvocationDoesNotExist):
        data_store.invocations.get(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,  # wrong function name for this invocation
            invocation_id=INVOCATION_ID_2,
        )


def test_create_invocation_with_duplicate_id(data_store: DataStore) -> None:
    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        parent_invocation=None,
        input=INPUT_1,
        cancellation_requested=False,
        invocation_status=InvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME,
    )

    # Try to create another invocation, but with the same ID
    with pytest.raises(InvocationAlreadyExists):
        data_store.invocations.create(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            parent_invocation=None,
            input=INPUT_2,
            cancellation_requested=False,
            invocation_status=InvocationStatus.RUNNING,
            creation_time=TIME,
            last_update_time=TIME,
        )

    invocation_1 = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    )
    assert invocation_1.input == INPUT_1  # the original input


def test_create_invocations_with_parent_child_relationship(
    data_store: DataStore,
) -> None:
    # Create parent
    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        parent_invocation=None,
        input=INPUT_1,
        cancellation_requested=True,
        invocation_status=InvocationStatus.TERMINATED,
        creation_time=TIME,
        last_update_time=TIME,
    )

    # Create child
    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        invocation_id=INVOCATION_ID_2,
        parent_invocation=ParentInvocationDefinition(
            function_name=FUNCTION_NAME_1, invocation_id=INVOCATION_ID_1
        ),
        input=INPUT_2,
        cancellation_requested=False,  # make all these values different from the parent
        invocation_status=InvocationStatus.RUNNING,
        creation_time=LATER_TIME,
        last_update_time=LATER_TIME,
    )

    child_invocation = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        invocation_id=INVOCATION_ID_2,
    )
    assert child_invocation.parent_invocation is not None
    assert child_invocation.parent_invocation.function_name == FUNCTION_NAME_1
    assert child_invocation.parent_invocation.invocation_id == INVOCATION_ID_1
    assert child_invocation.parent_invocation.cancellation_requested == True
    assert (
        child_invocation.parent_invocation.invocation_status
        == InvocationStatus.TERMINATED
    )
    assert child_invocation.parent_invocation.creation_time == TIME
    assert child_invocation.parent_invocation.last_update_time == TIME

    children_of_parent = data_store.invocations.list_for_function(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_2,
        parent_invocation=ParentInvocationDefinition(
            function_name=FUNCTION_NAME_1, invocation_id=INVOCATION_ID_1
        ),
    ).invocations
    assert len(children_of_parent) == 1
    assert children_of_parent[0].invocation_id == INVOCATION_ID_2
    assert children_of_parent[0].parent_invocation is not None
    assert children_of_parent[0].parent_invocation.function_name == FUNCTION_NAME_1

    invocations_in_running_status = data_store.invocations.list_all(
        statuses={InvocationStatus.RUNNING}
    )
    assert len(invocations_in_running_status) == 1
    assert invocations_in_running_status[0].parent_invocation is not None
    assert (
        invocations_in_running_status[0].parent_invocation.function_name
        == FUNCTION_NAME_1
    )
    assert (
        invocations_in_running_status[0].parent_invocation.invocation_id
        == INVOCATION_ID_1
    )
    assert (
        invocations_in_running_status[0].parent_invocation.cancellation_requested
        == True
    )
    assert (
        invocations_in_running_status[0].parent_invocation.invocation_status
        == InvocationStatus.TERMINATED
    )
    assert invocations_in_running_status[0].parent_invocation.creation_time == TIME
    assert invocations_in_running_status[0].parent_invocation.last_update_time == TIME


def test_create_invocations_with_nonexistent_parent(data_store: DataStore) -> None:
    # Create invocation, where the parent invocation doesn't exist
    with pytest.raises(ParentInvocationDoesNotExist):
        data_store.invocations.create(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_2,
            invocation_id=INVOCATION_ID_2,
            parent_invocation=ParentInvocationDefinition(
                function_name=FUNCTION_NAME_1, invocation_id=INVOCATION_ID_1
            ),
            input=INPUT_2,
            cancellation_requested=False,
            invocation_status=InvocationStatus.RUNNING,
            creation_time=LATER_TIME,
            last_update_time=LATER_TIME,
        )


def test_update_status(data_store: DataStore) -> None:
    # Create the invocation that we're going to update
    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        parent_invocation=None,
        input=INPUT_1,
        cancellation_requested=False,
        invocation_status=InvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME,
    )

    # Create another invocation that should be left untouched
    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_2,
        parent_invocation=None,
        input=INPUT_1,
        cancellation_requested=False,
        invocation_status=InvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME,
    )

    data_store.invocations.update(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        update_time=LATER_TIME,
        new_invocation_status=InvocationStatus.TERMINATED,
    )

    # The first invocation should have been updated
    invocation_1 = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    )
    assert invocation_1.last_update_time == LATER_TIME
    assert invocation_1.invocation_status == InvocationStatus.TERMINATED

    # The second invocation should remain untouched
    invocation_2 = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_2,
    )
    assert invocation_2.last_update_time == TIME
    assert invocation_2.invocation_status == InvocationStatus.RUNNING

    invocations_in_running_status = data_store.invocations.list_all(
        statuses={InvocationStatus.RUNNING}
    )
    assert len(invocations_in_running_status) == 1
    assert invocations_in_running_status[0].invocation_id == INVOCATION_ID_2

    invocations_in_terminated_status = data_store.invocations.list_all(
        statuses={InvocationStatus.TERMINATED}
    )
    assert len(invocations_in_terminated_status) == 1
    assert invocations_in_terminated_status[0].invocation_id == INVOCATION_ID_1


def test_update_cancellation_requested_flag(data_store: DataStore) -> None:
    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        parent_invocation=None,
        input=INPUT_1,
        cancellation_requested=False,
        invocation_status=InvocationStatus.RUNNING,
        creation_time=TIME,
        last_update_time=TIME,
    )

    data_store.invocations.update(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        update_time=LATER_TIME,
        set_cancellation_requested=True,
    )

    invocation_1 = data_store.invocations.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
    )
    assert invocation_1.last_update_time == LATER_TIME
    assert invocation_1.cancellation_requested == True


def test_update_cancellation_when_invocation_does_not_exist(
    data_store: DataStore,
) -> None:
    with pytest.raises(InvocationDoesNotExist):
        data_store.invocations.update(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            update_time=LATER_TIME,
            set_cancellation_requested=True,
        )


def test_list_invocations_for_function_using_max_results_and_initial_offset(
    data_store: DataStore,
) -> None:
    # Create two invocations of the same function
    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_1,
        parent_invocation=None,
        input=INPUT_1,
        cancellation_requested=True,
        invocation_status=InvocationStatus.TERMINATED,
        creation_time=TIME,
        last_update_time=TIME,
    )

    data_store.invocations.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        invocation_id=INVOCATION_ID_2,
        parent_invocation=None,
        input=INPUT_2,
        cancellation_requested=False,
        invocation_status=InvocationStatus.RUNNING,
        creation_time=LATER_TIME,
        last_update_time=LATER_TIME,
    )

    # Iterate through the table, with max_results = 1 for each page.
    # Note that the results should be returned in descending order of creation time.
    first_page = data_store.invocations.list_for_function(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        max_results=1,
    )
    assert len(first_page.invocations) == 1
    assert first_page.invocations[0].invocation_id == INVOCATION_ID_2

    second_page = data_store.invocations.list_for_function(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        max_results=1,
        initial_offset=first_page.next_offset,
    )
    assert len(second_page.invocations) == 1
    assert second_page.invocations[0].invocation_id == INVOCATION_ID_1
    assert second_page.next_offset is None

    # Iterate through table once more, but this time, with max_results = 10 for each page.
    # So this time, all the results should fit on a single page.
    single_page = data_store.invocations.list_for_function(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID,
        function_name=FUNCTION_NAME_1,
        max_results=10,
    )
    assert len(single_page.invocations) == 2
    assert single_page.invocations[0].invocation_id == INVOCATION_ID_2
    assert single_page.invocations[1].invocation_id == INVOCATION_ID_1


def test_methods_when_function_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(FunctionDoesNotExist):
        data_store.invocations.create(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=NONEXISTENT_FUNCTION_NAME,
            invocation_id=INVOCATION_ID_1,
            parent_invocation=None,
            input=INPUT_1,
            cancellation_requested=True,
            invocation_status=InvocationStatus.TERMINATED,
            creation_time=TIME,
            last_update_time=TIME,
        )

    with pytest.raises(FunctionDoesNotExist):
        data_store.invocations.update(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=NONEXISTENT_FUNCTION_NAME,
            invocation_id=INVOCATION_ID_1,
            update_time=LATER_TIME,
            set_cancellation_requested=True,
        )

    with pytest.raises(FunctionDoesNotExist):
        data_store.invocations.get(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=NONEXISTENT_FUNCTION_NAME,
            invocation_id=INVOCATION_ID_1,
        )

    with pytest.raises(FunctionDoesNotExist):
        data_store.invocations.list_for_function(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=NONEXISTENT_FUNCTION_NAME,
        )


def test_methods_when_project_version_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(VersionDoesNotExist):
        data_store.invocations.create(
            project_name=PROJECT_NAME,
            version_id=NONEXISTENT_VERSION_NAME,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            parent_invocation=None,
            input=INPUT_1,
            cancellation_requested=True,
            invocation_status=InvocationStatus.TERMINATED,
            creation_time=TIME,
            last_update_time=TIME,
        )

    with pytest.raises(VersionDoesNotExist):
        data_store.invocations.update(
            project_name=PROJECT_NAME,
            version_id=NONEXISTENT_VERSION_NAME,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            update_time=LATER_TIME,
            set_cancellation_requested=True,
        )

    with pytest.raises(VersionDoesNotExist):
        data_store.invocations.get(
            project_name=PROJECT_NAME,
            version_id=NONEXISTENT_VERSION_NAME,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
        )

    with pytest.raises(VersionDoesNotExist):
        data_store.invocations.list_for_function(
            project_name=PROJECT_NAME,
            version_id=NONEXISTENT_VERSION_NAME,
            function_name=FUNCTION_NAME_1,
        )


def test_methods_when_project_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(ProjectDoesNotExist):
        data_store.invocations.create(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            parent_invocation=None,
            input=INPUT_1,
            cancellation_requested=True,
            invocation_status=InvocationStatus.TERMINATED,
            creation_time=TIME,
            last_update_time=TIME,
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.invocations.update(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
            update_time=LATER_TIME,
            set_cancellation_requested=True,
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.invocations.get(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
            invocation_id=INVOCATION_ID_1,
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.invocations.list_for_function(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID,
            function_name=FUNCTION_NAME_1,
        )
