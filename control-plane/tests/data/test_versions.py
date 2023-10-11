from typing import Iterable

import pytest as pytest

from control_plane.data.data_store import DataStore
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import (
    ProjectDoesNotExist,
    VersionAlreadyExists,
    VersionDoesNotExist,
)


@pytest.fixture(scope="module")
def conn_pool() -> Iterable[SqlConnectionPool]:
    conn_pool = SqlConnectionPool.create_for_local_postgres()
    try:
        yield conn_pool
    finally:
        conn_pool.close()


PROJECT_NAME_1 = "project-1"
PROJECT_NAME_2 = "project-2"
NONEXISTENT_PROJECT_NAME = "nonexistent-project"

VERSION_ID_1 = "version-1"
VERSION_ID_2 = "version-2"

TIME = 0
LATER_TIME = 10


@pytest.fixture()
def data_store(conn_pool: SqlConnectionPool) -> Iterable[DataStore]:
    data_store = DataStore(conn_pool)
    data_store.create_tables()

    # Set up each test with the projects already inserted.
    data_store.projects.create(
        project_name=PROJECT_NAME_1, deletion_request_time=None, creation_time=TIME
    )
    data_store.projects.create(
        project_name=PROJECT_NAME_2, deletion_request_time=None, creation_time=TIME
    )

    try:
        yield data_store

    finally:
        data_store.delete_tables()


def test_create_two_versions(data_store: DataStore) -> None:
    # To begin with, no versions have been created.
    with pytest.raises(VersionDoesNotExist):
        data_store.project_versions.get(
            project_name=PROJECT_NAME_1, version_id=VERSION_ID_1
        )

    with pytest.raises(VersionDoesNotExist):
        data_store.project_versions.get_id_of_latest_version(
            project_name=PROJECT_NAME_1
        )

    versions_for_this_project = data_store.project_versions.list_for_project(
        project_name=PROJECT_NAME_1
    ).versions
    assert len(versions_for_this_project) == 0

    # Create first version
    data_store.project_versions.create(
        project_name=PROJECT_NAME_1, version_id=VERSION_ID_1, creation_time=TIME
    )

    version = data_store.project_versions.get(
        project_name=PROJECT_NAME_1, version_id=VERSION_ID_1
    )
    assert version.project_name == PROJECT_NAME_1
    assert version.version_id == VERSION_ID_1
    assert version.creation_time == TIME

    with pytest.raises(VersionDoesNotExist):
        data_store.project_versions.get(
            project_name=PROJECT_NAME_2,  # the other project
            version_id=VERSION_ID_1,  # but the same version ID
        )

    versions_for_this_project = data_store.project_versions.list_for_project(
        project_name=PROJECT_NAME_1
    ).versions
    assert len(versions_for_this_project) == 1
    assert versions_for_this_project[0].version_id == VERSION_ID_1
    assert versions_for_this_project[0].creation_time == TIME

    versions_for_other_project = data_store.project_versions.list_for_project(
        project_name=PROJECT_NAME_2
    ).versions
    assert len(versions_for_other_project) == 0

    latest_version_id = data_store.project_versions.get_id_of_latest_version(
        project_name=PROJECT_NAME_1
    )
    assert latest_version_id == VERSION_ID_1

    # Create second version, with a later timestamp
    data_store.project_versions.create(
        project_name=PROJECT_NAME_1, version_id=VERSION_ID_2, creation_time=LATER_TIME
    )

    version_1 = data_store.project_versions.get(
        project_name=PROJECT_NAME_1, version_id=VERSION_ID_1
    )
    assert version_1.version_id == VERSION_ID_1

    version_2 = data_store.project_versions.get(
        project_name=PROJECT_NAME_1, version_id=VERSION_ID_2
    )
    assert version_2.version_id == VERSION_ID_2

    versions = data_store.project_versions.list_for_project(
        project_name=PROJECT_NAME_1
    ).versions
    assert len(versions) == 2
    assert {version.version_id for version in versions} == {VERSION_ID_1, VERSION_ID_2}

    latest_version_id = data_store.project_versions.get_id_of_latest_version(
        project_name=PROJECT_NAME_1
    )
    assert latest_version_id == VERSION_ID_2


def test_create_with_duplicate_id(data_store: DataStore) -> None:
    data_store.project_versions.create(
        project_name=PROJECT_NAME_1, version_id=VERSION_ID_1, creation_time=TIME
    )

    # Try to use the same version id again
    with pytest.raises(VersionAlreadyExists):
        data_store.project_versions.create(
            project_name=PROJECT_NAME_1,
            version_id=VERSION_ID_1,
            creation_time=LATER_TIME,
        )

    versions_for_this_project = data_store.project_versions.list_for_project(
        project_name=PROJECT_NAME_1
    ).versions
    assert len(versions_for_this_project) == 1
    assert versions_for_this_project[0].creation_time == TIME


def test_methods_when_project_does_not_exist(data_store: DataStore) -> None:
    with pytest.raises(ProjectDoesNotExist):
        data_store.project_versions.create(
            project_name=NONEXISTENT_PROJECT_NAME,
            version_id=VERSION_ID_1,
            creation_time=TIME,
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.project_versions.get(
            project_name=NONEXISTENT_PROJECT_NAME, version_id=VERSION_ID_1
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.project_versions.get_id_of_latest_version(
            project_name=NONEXISTENT_PROJECT_NAME
        )

    with pytest.raises(ProjectDoesNotExist):
        data_store.project_versions.list_for_project(
            project_name=NONEXISTENT_PROJECT_NAME
        )
