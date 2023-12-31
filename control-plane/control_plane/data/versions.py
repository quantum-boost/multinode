import psycopg2

from control_plane.data.error_helpers import raise_error_if_project_does_not_exist
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import (
    ProjectDoesNotExist,
    VersionAlreadyExists,
    VersionDoesNotExist,
)
from control_plane.types.datatypes import (
    ExecutionSpec,
    FunctionInfoForVersion,
    FunctionStatus,
    PreparedFunctionDetails,
    ResourceSpec,
    VersionInfo,
    VersionInfoForProject,
    VersionsListForProject,
)


class VersionsTable:
    def __init__(self, pool: SqlConnectionPool) -> None:
        self._pool = pool

    def _create_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS project_versions (
                  project_name TEXT NOT NULL REFERENCES projects(project_name) ON DELETE CASCADE ON UPDATE CASCADE,
                  version_id TEXT NOT NULL,
                  creation_time INTEGER NOT NULL,
                  PRIMARY KEY (project_name, version_id)
                );
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS project_versions_ordering
                ON project_versions(project_name ASC, creation_time DESC, version_id ASC);
                """
            )

    def _delete_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                DROP INDEX IF EXISTS project_versions_ordering;
                """
            )

            cursor.execute(
                """
                DROP TABLE IF EXISTS project_versions;
                """
            )

    def create(self, *, project_name: str, version_id: str, creation_time: int) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionAlreadyExists:
        """
        raise_error_if_project_does_not_exist(project_name, self._pool)

        with self._pool.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    INSERT INTO project_versions
                    VALUES (%s, %s, %s);
                    """,
                    [project_name, version_id, creation_time],
                )
            except psycopg2.errors.UniqueViolation:
                raise VersionAlreadyExists
            except psycopg2.errors.ForeignKeyViolation:
                # Handle rare race condition in case a project deletion is happening concurrently.
                raise ProjectDoesNotExist

    def get_id_of_latest_version(self, *, project_name: str) -> str:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        """
        raise_error_if_project_does_not_exist(project_name, self._pool)

        with self._pool.cursor() as cursor:
            # Note on ordering: Really we want to order by creation_time, using version_id as a tie-breaker.
            # However, we'll write it as ORDER BY project_name ASC, creation_time DESC, version_id ASC,
            # making it explicit to Postgres that we want to use the project_versions_ordering index.
            # Of course, in reality, there is only one possible value for project_name: the value we pass in.
            cursor.execute(
                """
                SELECT version_id
                FROM project_versions
                WHERE project_name = %s
                ORDER BY project_name ASC, creation_time DESC, version_id ASC
                LIMIT 1;
                """,
                [project_name],
            )

            row = cursor.fetchone()

            if row is None:
                raise VersionDoesNotExist

            version_id: str = row[0]
            return version_id

    def get(self, *, project_name: str, version_id: str) -> VersionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        """
        raise_error_if_project_does_not_exist(project_name, self._pool)

        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  project_name,
                  version_id,
                  creation_time
                FROM project_versions
                WHERE project_name = %s AND version_id = %s;
                """,
                [project_name, version_id],
            )

            row = cursor.fetchone()

            if row is None:
                raise VersionDoesNotExist

            version = VersionInfo(
                project_name=row[0],
                version_id=row[1],
                creation_time=row[2],
                functions=[],  # will populate in a moment...
            )

            # Now fetch the functions for this project version...
            cursor.execute(
                """
                SELECT
                  function_name,
                  docker_image,
                  resource_spec,
                  execution_spec,
                  function_status,
                  prepared_function_details
                FROM functions
                WHERE project_name = %s AND version_id = %s;
                """,
                [project_name, version_id],
            )

            rows = cursor.fetchall()

            for row in rows:
                version.functions.append(
                    FunctionInfoForVersion(
                        function_name=row[0],
                        docker_image=row[1],
                        resource_spec=ResourceSpec.model_validate_json(row[2]),
                        execution_spec=ExecutionSpec.model_validate_json(row[3]),
                        function_status=FunctionStatus(row[4]),
                        prepared_function_details=(
                            PreparedFunctionDetails.model_validate_json(row[5])
                            if row[5] is not None
                            else None
                        ),
                    )
                )

            return version

    def list_for_project(self, *, project_name: str) -> VersionsListForProject:
        """
        :raises ProjectDoesNotExist:
        """
        raise_error_if_project_does_not_exist(project_name, self._pool)

        with self._pool.cursor() as cursor:
            # Again, the project_name ASC in the ORDER BY clause is logically redundant, but it acts as an
            # explicit cue to Postgres to use the project_versions_ordering index.
            cursor.execute(
                """
                SELECT
                  version_id,
                  creation_time
                FROM project_versions
                WHERE project_name = %s
                ORDER BY project_name ASC, creation_time DESC, version_id ASC;
                """,
                [project_name],
            )

            rows = cursor.fetchall()

            versions = [
                VersionInfoForProject(version_id=row[0], creation_time=row[1])
                for row in rows
            ]

            return VersionsListForProject(project_name=project_name, versions=versions)
