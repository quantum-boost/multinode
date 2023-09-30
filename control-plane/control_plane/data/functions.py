from typing import Optional, Any, Tuple

import psycopg2

from control_plane.data.error_helpers import raise_error_if_version_does_not_exist
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.datatypes import (
    FunctionInfo,
    FunctionsListForVersion,
    FunctionStatus,
    PreparedFunctionDetails,
    ResourceSpec,
    ExecutionSpec,
    FunctionInfoForVersion,
)
from control_plane.types.errortypes import (
    FunctionAlreadyExists,
    FunctionDoesNotExist,
    VersionDoesNotExist,
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
                    ON DELETE RESTRICT ON UPDATE RESTRICT
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
                # Future-proofing, in case we ever implement project or project version deletions.
                # If so, then there is an extremely rare race condition that needs to be handled.
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
                set_statement_args.append(new_prepared_function_details.model_dump_json())

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

    def get(self, *, project_name: str, version_id: str, function_name: str) -> FunctionInfo:
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
                  project_name,
                  version_id,
                  function_name,
                  docker_image,
                  resource_spec,
                  execution_spec,
                  function_status,
                  prepared_function_details
                FROM functions
                WHERE project_name = %s AND version_id = %s AND function_name = %s;
                """,
                [project_name, version_id, function_name],
            )

            row = cursor.fetchone()

            if row is None:
                raise FunctionDoesNotExist

            return _construct_function_info_from_row(row)

    def list_for_project_version(self, *, project_name: str, version_id: str) -> FunctionsListForVersion:
        """
        :raises ProjectDoesNotExist:
        :raises VersionDoesNotExist:
        """
        raise_error_if_version_does_not_exist(project_name, version_id, self._pool)

        with self._pool.cursor() as cursor:
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

            functions = [
                FunctionInfoForVersion(
                    function_name=row[0],
                    docker_image=row[1],
                    resource_spec=ResourceSpec.model_validate_json(row[2]),
                    execution_spec=ExecutionSpec.model_validate_json(row[3]),
                    function_status=FunctionStatus(row[4]),
                    prepared_function_details=(
                        PreparedFunctionDetails.model_validate_json(row[5]) if row[5] is not None else None
                    ),
                )
                for row in rows
            ]

            return FunctionsListForVersion(
                project_name=project_name,
                version_id=version_id,
                functions=functions,
            )

    def list_all(self, *, statuses: set[FunctionStatus]) -> list[FunctionInfo]:
        if len(statuses) == 0:
            # Must handle separately to avoid SQL syntax error
            return []

        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  project_name,
                  version_id,
                  function_name,
                  docker_image,
                  resource_spec,
                  execution_spec,
                  function_status,
                  prepared_function_details
                FROM functions
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
        prepared_function_details=(PreparedFunctionDetails.model_validate_json(row[7]) if row[7] is not None else None),
    )
