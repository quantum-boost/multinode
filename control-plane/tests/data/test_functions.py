from typing import Iterable

import pytest

from control_plane.data.data_store import DataStore
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.datatypes import (
    ResourceSpec,
    ExecutionSpec,
    FunctionStatus,
    PreparedFunctionDetails,
    WorkerType,
)
from control_plane.types.errortypes import (
    FunctionDoesNotExist,
    FunctionAlreadyExists,
    VersionDoesNotExist,
    ProjectDoesNotExist,
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

VERSION_ID_1 = "version-1"
VERSION_ID_2 = "version-2"
NONEXISTENT_VERSION_ID = "nonexistent-version"

FUNCTION_NAME_1 = "function-1"
FUNCTION_NAME_2 = "function-2"

DOCKER_IMAGE = "image"
RESOURCE_SPEC = ResourceSpec(virtual_cpus=1.0, memory_gbs=4.0, max_concurrency=5)
EXECUTION_SPEC = ExecutionSpec(timeout_seconds=300, max_retries=8)
PREPARED_FUNCTION_DETAILS = PreparedFunctionDetails(type=WorkerType.TEST, identifier="def")

TIME = 0


@pytest.fixture()
def data_store(conn_pool: SqlConnectionPool) -> Iterable[DataStore]:
    data_store = DataStore(conn_pool)
    data_store.create_tables()

    # Set up each test with the project and versions already inserted.
    data_store.projects.create(project_name=PROJECT_NAME, creation_time=TIME)
    data_store.project_versions.create(project_name=PROJECT_NAME, version_id=VERSION_ID_1, creation_time=TIME)
    data_store.project_versions.create(project_name=PROJECT_NAME, version_id=VERSION_ID_2, creation_time=TIME)

    try:
        yield data_store

    finally:
        data_store.delete_tables()


def test_create_two_functions(data_store: DataStore) -> None:
    # To begin with, no functions have been created.
    with pytest.raises(FunctionDoesNotExist):
        data_store.functions.get(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID_1,
            function_name=FUNCTION_NAME_1,
        )

    functions_for_version = data_store.functions.list_for_project_version(
        project_name=PROJECT_NAME, version_id=VERSION_ID_1
    ).functions
    assert len(functions_for_version) == 0

    version = data_store.project_versions.get(project_name=PROJECT_NAME, version_id=VERSION_ID_1)
    assert len(version.functions) == 0

    # Create first function
    data_store.functions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_1,
        docker_image=DOCKER_IMAGE,
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=FunctionStatus.PENDING,
        prepared_function_details=None,
    )

    function = data_store.functions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_1,
    )
    assert function.project_name == PROJECT_NAME
    assert function.version_id == VERSION_ID_1
    assert function.function_name == FUNCTION_NAME_1
    assert function.docker_image == DOCKER_IMAGE
    assert function.resource_spec.virtual_cpus == pytest.approx(RESOURCE_SPEC.virtual_cpus, 1.0e-5)
    assert function.execution_spec == EXECUTION_SPEC
    assert function.function_status == FunctionStatus.PENDING
    assert function.prepared_function_details is None

    all_functions = data_store.functions.list_all(statuses={FunctionStatus.PENDING})
    assert len(all_functions) == 1
    assert all_functions[0].project_name == PROJECT_NAME
    assert all_functions[0].version_id == VERSION_ID_1
    assert all_functions[0].function_name == FUNCTION_NAME_1
    assert all_functions[0].docker_image == DOCKER_IMAGE
    assert all_functions[0].resource_spec.virtual_cpus == pytest.approx(RESOURCE_SPEC.virtual_cpus, 1.0e-5)
    assert all_functions[0].execution_spec == EXECUTION_SPEC
    assert all_functions[0].function_status == FunctionStatus.PENDING
    assert all_functions[0].prepared_function_details is None

    functions_for_version = data_store.functions.list_for_project_version(
        project_name=PROJECT_NAME, version_id=VERSION_ID_1
    ).functions
    assert len(functions_for_version) == 1
    assert functions_for_version[0].function_name == FUNCTION_NAME_1
    assert functions_for_version[0].docker_image == DOCKER_IMAGE
    assert functions_for_version[0].resource_spec.virtual_cpus == pytest.approx(RESOURCE_SPEC.virtual_cpus, 1.0e-5)
    assert functions_for_version[0].execution_spec == EXECUTION_SPEC
    assert functions_for_version[0].function_status == FunctionStatus.PENDING
    assert functions_for_version[0].prepared_function_details is None

    version = data_store.project_versions.get(project_name=PROJECT_NAME, version_id=VERSION_ID_1)
    assert len(version.functions) == 1
    assert version.functions[0].function_name == FUNCTION_NAME_1
    assert version.functions[0].docker_image == DOCKER_IMAGE
    assert version.functions[0].resource_spec.virtual_cpus == pytest.approx(RESOURCE_SPEC.virtual_cpus, 1.0e-5)
    assert version.functions[0].execution_spec == EXECUTION_SPEC
    assert version.functions[0].function_status == FunctionStatus.PENDING
    assert version.functions[0].prepared_function_details is None

    functions_for_other_version = data_store.functions.list_for_project_version(
        project_name=PROJECT_NAME, version_id=VERSION_ID_2  # the other project version
    ).functions
    assert len(functions_for_other_version) == 0

    other_version = data_store.project_versions.get(project_name=PROJECT_NAME, version_id=VERSION_ID_2)
    assert len(other_version.functions) == 0

    # Create second function
    data_store.functions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_2,
        docker_image=DOCKER_IMAGE,
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=FunctionStatus.PENDING,
        prepared_function_details=None,
    )

    function_1 = data_store.functions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_1,
    )
    assert function_1.function_name == FUNCTION_NAME_1

    function_2 = data_store.functions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_2,
    )
    assert function_2.function_name == FUNCTION_NAME_2

    all_functions = data_store.functions.list_all(statuses={FunctionStatus.PENDING})
    assert len(all_functions) == 2
    assert {function.function_name for function in all_functions} == {
        FUNCTION_NAME_1,
        FUNCTION_NAME_2,
    }

    functions_for_version = data_store.functions.list_for_project_version(
        project_name=PROJECT_NAME, version_id=VERSION_ID_1
    ).functions
    assert len(functions_for_version) == 2
    assert {function.function_name for function in functions_for_version} == {
        FUNCTION_NAME_1,
        FUNCTION_NAME_2,
    }

    version = data_store.project_versions.get(project_name=PROJECT_NAME, version_id=VERSION_ID_1)
    assert len(version.functions) == 2
    assert {function.function_name for function in version.functions} == {
        FUNCTION_NAME_1,
        FUNCTION_NAME_2,
    }


def test_create_with_duplicate_name(data_store: DataStore) -> None:
    data_store.functions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_1,
        docker_image=DOCKER_IMAGE,
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=FunctionStatus.PENDING,
        prepared_function_details=None,
    )

    # Now try to use the same function name a second time.
    with pytest.raises(FunctionAlreadyExists):
        data_store.functions.create(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID_1,
            function_name=FUNCTION_NAME_1,
            docker_image=DOCKER_IMAGE,
            resource_spec=RESOURCE_SPEC,
            execution_spec=EXECUTION_SPEC,
            function_status=FunctionStatus.PENDING,
            prepared_function_details=None,
        )

    functions_for_version = data_store.functions.list_for_project_version(
        project_name=PROJECT_NAME, version_id=VERSION_ID_1
    ).functions
    assert len(functions_for_version) == 1


def test_update_function(data_store: DataStore) -> None:
    # Create the function that we are going to update
    data_store.functions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_1,
        docker_image=DOCKER_IMAGE,
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=FunctionStatus.PENDING,
        prepared_function_details=None,
    )

    # Create some other function that should be left untouched by the update
    data_store.functions.create(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_2,
        docker_image=DOCKER_IMAGE,
        resource_spec=RESOURCE_SPEC,
        execution_spec=EXECUTION_SPEC,
        function_status=FunctionStatus.PENDING,
        prepared_function_details=None,
    )

    # Perform an update
    data_store.functions.update(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_1,
        new_function_status=FunctionStatus.READY,
        new_prepared_function_details=PREPARED_FUNCTION_DETAILS,
    )

    # The function that got updated should be updated
    updated_function = data_store.functions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_1,
    )
    assert updated_function.function_status == FunctionStatus.READY
    assert updated_function.prepared_function_details == PREPARED_FUNCTION_DETAILS

    # Meanwhile, the other function should be left untouched
    other_function = data_store.functions.get(
        project_name=PROJECT_NAME,
        version_id=VERSION_ID_1,
        function_name=FUNCTION_NAME_2,
    )
    assert other_function.function_status == FunctionStatus.PENDING
    assert other_function.prepared_function_details is None

    functions_for_version = data_store.functions.list_for_project_version(
        project_name=PROJECT_NAME, version_id=VERSION_ID_1
    ).functions
    assert (
        functions_for_version[0].prepared_function_details == PREPARED_FUNCTION_DETAILS
        or functions_for_version[1].prepared_function_details == PREPARED_FUNCTION_DETAILS
    )

    version = data_store.project_versions.get(project_name=PROJECT_NAME, version_id=VERSION_ID_1)
    assert (
        version.functions[0].prepared_function_details == PREPARED_FUNCTION_DETAILS
        or version.functions[1].prepared_function_details == PREPARED_FUNCTION_DETAILS
    )

    functions_in_pending_status = data_store.functions.list_all(statuses={FunctionStatus.PENDING})
    assert len(functions_in_pending_status) == 1
    assert functions_in_pending_status[0].function_name == FUNCTION_NAME_2

    functions_in_ready_status = data_store.functions.list_all(statuses={FunctionStatus.READY})
    assert len(functions_in_ready_status) == 1
    assert functions_in_ready_status[0].function_name == FUNCTION_NAME_1
    assert functions_in_ready_status[0].prepared_function_details == PREPARED_FUNCTION_DETAILS


def test_update_when_function_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(FunctionDoesNotExist):
        data_store.functions.update(
            project_name=PROJECT_NAME,
            version_id=VERSION_ID_1,
            function_name=FUNCTION_NAME_1,
            new_function_status=FunctionStatus.READY,
            new_prepared_function_details=PREPARED_FUNCTION_DETAILS,
        )


def test_methods_when_project_version_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(VersionDoesNotExist):
        data_store.functions.create(
            project_name=PROJECT_NAME,
            version_id=NONEXISTENT_VERSION_ID,
            function_name=FUNCTION_NAME_1,
            docker_image=DOCKER_IMAGE,
            resource_spec=RESOURCE_SPEC,
            execution_spec=EXECUTION_SPEC,
            function_status=FunctionStatus.PENDING,
            prepared_function_details=None,
        )

    with pytest.raises(VersionDoesNotExist):
        data_store.functions.update(
            project_name=PROJECT_NAME,
            version_id=NONEXISTENT_VERSION_ID,
            function_name=FUNCTION_NAME_1,
            new_function_status=FunctionStatus.READY,
            new_prepared_function_details=PREPARED_FUNCTION_DETAILS,
        )

    with pytest.raises(VersionDoesNotExist):
        data_store.functions.get(
            project_name=PROJECT_NAME,
            version_id=NONEXISTENT_VERSION_ID,
            function_name=FUNCTION_NAME_1,
        )

    with pytest.raises(VersionDoesNotExist):
        data_store.functions.list_for_project_version(project_name=PROJECT_NAME, version_id=NONEXISTENT_VERSION_ID)


def test_methods_when_project_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(ProjectDoesNotExist):
        data_store.functions.create(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID_1,
            function_name=FUNCTION_NAME_1,
            docker_image=DOCKER_IMAGE,
            resource_spec=RESOURCE_SPEC,
            execution_spec=EXECUTION_SPEC,
            function_status=FunctionStatus.PENDING,
            prepared_function_details=None,
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.functions.update(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID_1,
            function_name=FUNCTION_NAME_1,
            new_function_status=FunctionStatus.READY,
            new_prepared_function_details=PREPARED_FUNCTION_DETAILS,
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.functions.get(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID_1,
            function_name=FUNCTION_NAME_1,
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.functions.list_for_project_version(project_name=NONEXISTENT_PROJECT_NAME, version_id=VERSION_ID_1)
