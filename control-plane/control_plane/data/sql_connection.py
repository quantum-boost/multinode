from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2 import pool


class SqlConnectionPool:
    def __init__(self, host: str, port: int, db: str, user: str, password: str):
        self._connection_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=20,
            host=host,
            port=port,
            database=db,
            user=user,
            password=password,
        )

    @contextmanager
    def cursor(self) -> Iterator[psycopg2.extensions.cursor]:
        connection = self._connection_pool.getconn()
        try:
            with connection:
                with connection.cursor() as cursor:
                    yield cursor
        finally:
            self._connection_pool.putconn(connection)

    def close(self) -> None:
        self._connection_pool.closeall()

    @classmethod
    def create_for_local_postgres(cls) -> "SqlConnectionPool":
        return cls(
            host="localhost", port=5432, db="db", user="user", password="password"
        )
