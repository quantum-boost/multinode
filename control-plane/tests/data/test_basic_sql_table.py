"""
A basic test setup for SQL.
When we write actual code, we will delete the tests but keep the useful boilerplate.
"""
from typing import Generator, Iterable

import psycopg2
import pytest as pytest

from control_plane.data.basic_sql_table import BasicSqlDataStore
from control_plane.data.sql_connection import get_local_postgres_connection


# The same connection will be used for all tests...
@pytest.fixture(scope='module')
def conn() -> Generator[psycopg2.extensions.connection, None, None]:
    conn = get_local_postgres_connection()
    try:
        yield conn
    finally:
        conn.close()


# ... but each test will use fresh data store object.
@pytest.fixture()
def data_store(conn: psycopg2.extensions.connection) -> Iterable[BasicSqlDataStore]:
    data_store = BasicSqlDataStore(conn)
    data_store.create_table()
    try:
        yield data_store
    finally:
        data_store.drop_table()


def test_add_then_get(data_store: BasicSqlDataStore) -> None:
    data_store.add_person("Alice", 20)
    age = data_store.get_person_age("Alice")
    assert age == 20
