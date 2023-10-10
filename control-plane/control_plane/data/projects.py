from typing import Any

import psycopg2

from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.api_errors import ProjectAlreadyExists, ProjectDoesNotExist
from control_plane.types.datatypes import ProjectInfo, ProjectsList


class ProjectsTable:
    def __init__(self, pool: SqlConnectionPool) -> None:
        self._pool = pool

    def _create_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                  project_name TEXT NOT NULL PRIMARY KEY,
                  deletion_requested BOOLEAN,
                  creation_time INTEGER NOT NULL
                );
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS projects_ordering
                ON projects(creation_time DESC, project_name ASC);
                """
            )

    def _delete_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                DROP INDEX IF EXISTS projects_ordering;
                """
            )

            cursor.execute(
                """
                DROP TABLE IF EXISTS projects;
                """
            )

    def create(
        self, *, project_name: str, deletion_requested: bool, creation_time: int
    ) -> None:
        """
        :raises ProjectAlreadyExists:
        """
        with self._pool.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    INSERT INTO projects
                    VALUES (%s, %s, %s);
                    """,
                    [project_name, deletion_requested, creation_time],
                )
            except psycopg2.errors.UniqueViolation:
                raise ProjectAlreadyExists

    def update(self, *, project_name: str, set_deletion_requested: bool) -> None:
        """
        :raises ProjectDoesNotExist:
        """
        with self._pool.cursor() as cursor:
            set_statement_clauses: list[str] = []
            set_statement_args: list[Any] = []

            if set_deletion_requested:
                set_statement_clauses.append("deletion_requested = %s")
                set_statement_args.append(True)

            if len(set_statement_clauses) == 0:
                # Running SQL statement with no SET clauses will raise a SQL syntax error
                return

            set_statement = "SET " + ", ".join(set_statement_clauses)

            cursor.execute(
                f"""
                UPDATE projects
                {set_statement}
                WHERE project_name = %s
                ;
                """,
                set_statement_args + [project_name],
            )

            if cursor.rowcount == 0:
                raise ProjectDoesNotExist

    def get(self, *, project_name: str) -> ProjectInfo:
        """
        :raises ProjectDoesNotExist:
        """
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  project_name,
                  deletion_requested,
                  creation_time
                FROM projects
                WHERE project_name = %s;
                """,
                [project_name],
            )

            row = cursor.fetchone()

            if row is None:
                raise ProjectDoesNotExist

            return ProjectInfo(
                project_name=row[0], deletion_requested=row[1], creation_time=row[2]
            )

    def list(self) -> ProjectsList:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  project_name,
                  deletion_requested,
                  creation_time
                FROM projects
                ORDER BY creation_time DESC, project_name ASC;
                """
            )

            rows = cursor.fetchall()

            projects = [
                ProjectInfo(
                    project_name=row[0], deletion_requested=row[1], creation_time=row[2]
                )
                for row in rows
            ]

            return ProjectsList(projects=projects)

    def delete_with_cascade(self, *, project_name: str) -> None:
        """
        This is a cascading deletion. As well as deleting the project, it also deletes every that has a foreign key
        relationship with this project (i.e. all versions, functions, invocations and executions associated with
        this project)

        :raises ProjectDoesNotExist:
        """
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                DELETE
                FROM projects
                WHERE project_name = %s;
                """,
                [project_name],
            )

            if cursor.rowcount == 0:
                raise ProjectDoesNotExist
