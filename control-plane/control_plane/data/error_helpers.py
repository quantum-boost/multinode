from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import (
    FunctionDoesNotExist,
    InvocationDoesNotExist,
    ProjectDoesNotExist,
    VersionDoesNotExist,
)


def project_exists(project_name: str, pool: SqlConnectionPool) -> bool:
    with pool.cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM projects
            WHERE project_name = %s;
            """,
            [project_name],
        )

        row = cursor.fetchone()
        return row is not None


def version_exists(project_name: str, version_id: str, pool: SqlConnectionPool) -> bool:
    with pool.cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM project_versions
            WHERE project_name = %s AND version_id = %s;
            """,
            [project_name, version_id],
        )

        row = cursor.fetchone()
        return row is not None


def function_exists(
    project_name: str,
    version_id: str,
    function_name: str,
    pool: SqlConnectionPool,
) -> bool:
    with pool.cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM functions
            WHERE project_name = %s AND version_id = %s AND function_name = %s;
            """,
            [project_name, version_id, function_name],
        )

        row = cursor.fetchone()
        return row is not None


def invocation_exists(
    project_name: str,
    version_id: str,
    function_name: str,
    invocation_id: str,
    pool: SqlConnectionPool,
) -> bool:
    with pool.cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM invocations
            WHERE project_name = %s AND version_id = %s AND function_name = %s AND invocation_id = %s;
            """,
            [project_name, version_id, function_name, invocation_id],
        )

        row = cursor.fetchone()
        return row is not None


def raise_error_if_project_does_not_exist(
    project_name: str, pool: SqlConnectionPool
) -> None:
    if not project_exists(project_name, pool):
        raise ProjectDoesNotExist


def raise_error_if_version_does_not_exist(
    project_name: str, version_id: str, pool: SqlConnectionPool
) -> None:
    if not version_exists(project_name, version_id, pool):
        # Must make sure to raise correct error.
        # It could be that the *project* doesn't exist, let alone the version.
        raise_error_if_project_does_not_exist(project_name, pool)
        raise VersionDoesNotExist


def raise_error_if_function_does_not_exist(
    project_name: str,
    version_id: str,
    function_name: str,
    pool: SqlConnectionPool,
) -> None:
    if not function_exists(project_name, version_id, function_name, pool):
        raise_error_if_version_does_not_exist(project_name, version_id, pool)
        raise FunctionDoesNotExist


def raise_error_if_invocation_does_not_exist(
    project_name: str,
    version_id: str,
    function_name: str,
    invocation_id: str,
    pool: SqlConnectionPool,
) -> None:
    if not invocation_exists(
        project_name, version_id, function_name, invocation_id, pool
    ):
        raise_error_if_function_does_not_exist(
            project_name, version_id, function_name, pool
        )
        raise InvocationDoesNotExist
