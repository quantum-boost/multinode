from typing import Any, Optional, Tuple

import psycopg2

from control_plane.data.error_helpers import raise_error_if_version_does_not_exist
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import (
    FunctionAlreadyExists,
    FunctionDoesNotExist,
    VersionDoesNotExist,
)
from control_plane.types.datatypes import (
    ExecutionSpec,
    FunctionInfo,
    FunctionInfoForVersion,
    FunctionsListForVersion,
    FunctionStatus,
    PreparedFunctionDetails,
    ResourceSpec,
)


class FunctionsTable:
    def __init__(self, pool: SqlConnectionPool) -> None:
        self._pool = pool

    def _create_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS functions (
                  project_name TEXT NOT NULL,
                  version_id TEXT NOT NULL,
                  function_name TEXT NOT NULL,
                  docker_image TEXT NOT NULL,
                  resource_spec TEXT NOT NULL,
                  execution_spec TEXT NOT NULL,
                  function_status TEXT NOT NULL,
                  prepared_function_details TEXT,
                  PRIMARY KEY (project_name, version_id, function_name),
                  FOREIGN KEY (project_name, version_id) REFERENCES project_versions(project_name, version_id)
                    ON DELETE CASCADE ON UPDATE CASCADE
                );
                """
            )

    def _delete_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                DROP TABLE IF EXISTS functions;
                """
            )

    def create(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        docker_image: str,
        resource_spec: ResourceSpec,
        execution_spec: ExecutionSpec,
        function_status: FunctionStatus,
        prepared_function_details: Optional[PreparedFunctionDetails],
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionAlreadyExists:
        """
        raise_error_if_version_does_not_exist(project_name, version_id, self._pool)

        with self._pool.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    INSERT INTO functions
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    [
                        project_name,
                        version_id,
                        function_name,
                        docker_image,
                        resource_spec.model_dump_json(),
                        execution_spec.model_dump_json(),
                        function_status.value,
                        (
                            prepared_function_details.model_dump_json()
                            if prepared_function_details is not None
                            else None
                        ),
                    ],
                )
            except psycopg2.errors.UniqueViolation:
                raise FunctionAlreadyExists
            except psycopg2.errors.ForeignKeyViolation:
                # Handle rare race condition in case a project deletion is happening concurrently.
                raise VersionDoesNotExist

    def update(
        self,
        *,
        project_name: str,
        version_id: str,
        function_name: str,
        new_function_status: Optional[FunctionStatus],
        new_prepared_function_details: Optional[PreparedFunctionDetails],
    ) -> None:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        """
        raise_error_if_version_does_not_exist(project_name, version_id, self._pool)

        with self._pool.cursor() as cursor:
            set_statement_clauses: list[str] = []
            set_statement_args: list[Any] = []

            if new_function_status is not None:
                set_statement_clauses.append("function_status = %s")
                set_statement_args.append(new_function_status.value)

            if new_prepared_function_details is not None:
                set_statement_clauses.append("prepared_function_details = %s")
                set_statement_args.append(
                    new_prepared_function_details.model_dump_json()
                )

            if len(set_statement_clauses) == 0:
                # Running SQL statement with no SET clauses will raise a SQL syntax error
                return

            set_statement = "SET " + ", ".join(set_statement_clauses)

            cursor.execute(
                f"""
                UPDATE functions
                {set_statement}
                WHERE project_name = %s AND version_id = %s AND function_name = %s
                ;
                """,
                set_statement_args + [project_name, version_id, function_name],
            )

            if cursor.rowcount == 0:
                raise FunctionDoesNotExist

    def get(
        self, *, project_name: str, version_id: str, function_name: str
    ) -> FunctionInfo:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        :raises FunctionDoesNotExist:
        """
        raise_error_if_version_does_not_exist(project_name, version_id, self._pool)

        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  functions.project_name,
                  functions.version_id,
                  functions.function_name,
                  functions.docker_image,
                  functions.resource_spec,
                  functions.execution_spec,
                  functions.function_status,
                  functions.prepared_function_details,
                  projects.deletion_request_time
                FROM projects
                INNER JOIN functions USING (project_name)
                WHERE project_name = %s AND version_id = %s AND function_name = %s;
                """,
                [project_name, version_id, function_name],
            )

            row = cursor.fetchone()

            if row is None:
                raise FunctionDoesNotExist

            return _construct_function_info_from_row(row)

    def list_all(self, *, statuses: set[FunctionStatus]) -> list[FunctionInfo]:
        if len(statuses) == 0:
            # Must handle separately to avoid SQL syntax error
            return []

        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  functions.project_name,
                  functions.version_id,
                  functions.function_name,
                  functions.docker_image,
                  functions.resource_spec,
                  functions.execution_spec,
                  functions.function_status,
                  functions.prepared_function_details,
                  projects.deletion_request_time
                FROM projects
                INNER JOIN functions USING (project_name)
                WHERE function_status IN %s;
                """,
                [tuple(status.value for status in statuses)],
            )

            rows = cursor.fetchall()

            return [_construct_function_info_from_row(row) for row in rows]


def _construct_function_info_from_row(row: Tuple[Any, ...]) -> FunctionInfo:
    return FunctionInfo(
        project_name=row[0],
        version_id=row[1],
        function_name=row[2],
        docker_image=row[3],
        resource_spec=ResourceSpec.model_validate_json(row[4]),
        execution_spec=ExecutionSpec.model_validate_json(row[5]),
        function_status=FunctionStatus(row[6]),
        prepared_function_details=(
            PreparedFunctionDetails.model_validate_json(row[7])
            if row[7] is not None
            else None
        ),
        project_deletion_request_time=row[8],
    )
