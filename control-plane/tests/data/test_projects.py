from typing import Iterable

import pytest as pytest

from control_plane.data.data_store import DataStore
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import ProjectAlreadyExists, ProjectDoesNotExist


# The same connection will be used for all tests...
@pytest.fixture(scope="module")
def conn_pool() -> Iterable[SqlConnectionPool]:
    conn_pool = SqlConnectionPool.create_for_local_postgres()
    try:
        yield conn_pool
    finally:
        conn_pool.close()


PROJECT_NAME_1 = "project-1"
PROJECT_NAME_2 = "project-2"

TIME = 0


# ... but each test will use fresh data store object.
@pytest.fixture()
def data_store(conn_pool: SqlConnectionPool) -> Iterable[DataStore]:
    data_store = DataStore(conn_pool)
    data_store.create_tables()
    try:
        yield data_store
    finally:
        data_store.delete_tables()


def test_create_two_projects(data_store: DataStore) -> None:
    # To begin with, no projects have been created.
    with pytest.raises(ProjectDoesNotExist):
        data_store.projects.get(project_name=PROJECT_NAME_1)

    assert len(data_store.projects.list().projects) == 0

    # Create project-1
    data_store.projects.create(
        project_name=PROJECT_NAME_1, deletion_request_time=None, creation_time=TIME
    )

    project_1 = data_store.projects.get(project_name=PROJECT_NAME_1)
    assert project_1.project_name == PROJECT_NAME_1
    assert project_1.deletion_requested == False
    assert project_1.creation_time == TIME

    projects = data_store.projects.list().projects
    assert len(projects) == 1
    assert projects[0].project_name == PROJECT_NAME_1
    assert projects[0].deletion_requested == False
    assert projects[0].creation_time == TIME

    # Create project-2
    data_store.projects.create(
        project_name=PROJECT_NAME_2, deletion_request_time=None, creation_time=TIME
    )

    project_1 = data_store.projects.get(project_name=PROJECT_NAME_1)
    assert project_1.project_name == PROJECT_NAME_1

    project_2 = data_store.projects.get(project_name=PROJECT_NAME_2)
    assert project_2.project_name == PROJECT_NAME_2

    projects = data_store.projects.list().projects
    assert len(projects) == 2
    assert {project.project_name for project in projects} == {
        PROJECT_NAME_1,
        PROJECT_NAME_2,
    }
    assert {project.creation_time for project in projects} == {TIME}


def test_create_project_with_duplicate_name(data_store: DataStore) -> None:
    data_store.projects.create(
        project_name=PROJECT_NAME_1, deletion_request_time=None, creation_time=TIME
    )

    # Try to use the same project name again
    with pytest.raises(ProjectAlreadyExists):
        data_store.projects.create(
            project_name=PROJECT_NAME_1, deletion_request_time=None, creation_time=20
        )

    assert len(data_store.projects.list().projects) == 1
    project_1 = data_store.projects.get(project_name=PROJECT_NAME_1)
    assert project_1.creation_time == TIME


def test_update_deletion_requested(data_store: DataStore) -> None:
    # Create the project that will be updated
    data_store.projects.create(
        project_name=PROJECT_NAME_1, deletion_request_time=None, creation_time=TIME
    )

    # Create another project that should be left untouched by the update
    data_store.projects.create(
        project_name=PROJECT_NAME_2, deletion_request_time=None, creation_time=TIME
    )

    # Update the first project, setting deletion_requested = True
    data_store.projects.update(
        project_name=PROJECT_NAME_1, new_deletion_request_time=TIME
    )

    # The first project should be updated
    project_1 = data_store.projects.get(project_name=PROJECT_NAME_1)
    assert project_1.deletion_request_time == TIME
    assert project_1.deletion_requested == True

    # The second project should be left untouched
    project_2 = data_store.projects.get(project_name=PROJECT_NAME_2)
    assert project_2.deletion_request_time is None
    assert project_2.deletion_requested == False


def test_update_when_project_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(ProjectDoesNotExist):
        data_store.projects.update(
            project_name=PROJECT_NAME_1, new_deletion_request_time=TIME
        )
