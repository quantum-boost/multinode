import psycopg2

from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.types.datatypes import ProjectInfo, ProjectsList
from control_plane.types.errortypes import ProjectAlreadyExists, ProjectDoesNotExist


class ProjectsTable:
    def __init__(self, pool: SqlConnectionPool) -> None:
        self._pool = pool

    def _create_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                  project_name TEXT NOT NULL PRIMARY KEY,
                  creation_time INTEGER NOT NULL
                );
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS projects_creation_time
                ON projects(creation_time);
                """
            )

    def _delete_table(self) -> None:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                DROP INDEX IF EXISTS projects_creation_time;
                """
            )

            cursor.execute(
                """
                DROP TABLE IF EXISTS projects;
                """
            )

    def create(self, *, project_name: str, creation_time: int) -> None:
        """
        :raises ProjectAlreadyExists:
        """
        with self._pool.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    INSERT INTO projects
                    VALUES (%s, %s);
                    """,
                    [project_name, creation_time],
                )
            except psycopg2.errors.UniqueViolation:
                raise ProjectAlreadyExists

    def get(self, *, project_name: str) -> ProjectInfo:
        """
        :raises ProjectDoesNotExist:
        """
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  project_name,
                  creation_time
                FROM projects
                WHERE project_name = %s;
                """,
                [project_name],
            )

            row = cursor.fetchone()

            if row is None:
                raise ProjectDoesNotExist

            return ProjectInfo(project_name=row[0], creation_time=row[1])

    def list(self) -> ProjectsList:
        with self._pool.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  project_name,
                  creation_time
                FROM projects
                ORDER BY creation_time DESC;
                """
            )

            rows = cursor.fetchall()

            projects = [ProjectInfo(project_name=row[0], creation_time=row[1]) for row in rows]

            return ProjectsList(projects=projects)
