"""
A basic SQL table - used to check that our CI is working.
Will delete when we have real code.
"""
import psycopg2


class BasicSqlDataStore:

    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self._conn = conn

    def create_table(self) -> None:
        with self._conn:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS people (
                      name TEXT PRIMARY KEY,
                      age INTEGER NOT NULL
                    );
                    """
                )

    def drop_table(self) -> None:
        with self._conn:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    DROP TABLE IF EXISTS people;
                    """
                )

    def add_person(self, name: str, age: int) -> None:
        with self._conn:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO people
                    VALUES (%s, %s);
                    """,
                    (name, age)
                )

    def get_person_age(self, name: str) -> int:
        with self._conn:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT age
                    FROM people
                    WHERE name = %s;
                    """,
                    (name,)
                )

                row = cursor.fetchone()
                assert row is not None
                age: int = row[0]
                return age
