import logging
from contextlib import contextmanager
from typing import Iterator

from control_plane.data.data_store import DataStore
from control_plane.data.sql_connection import SqlConnectionPool
from control_plane.entrypoints.utils.cli_arguments import CliArguments
from control_plane.entrypoints.utils.environment import (
    get_optional_environment_variable_with_default,
    get_mandatory_environment_variable,
)

POSTGRES_HOST_ENV = "POSTGRES_HOST"
POSTGRES_DB_ENV = "POSTGRES_DB"
POSTGRES_USER_ENV = "POSTGRES_USER"
POSTGRES_PASSWORD_ENV = "POSTGRES_PASSWORD"


@contextmanager
def datastore_from_environment_variables(cli_args: CliArguments) -> Iterator[DataStore]:
    host = get_optional_environment_variable_with_default(POSTGRES_HOST_ENV, "localhost")
    port = 5432
    db = get_mandatory_environment_variable(POSTGRES_DB_ENV)
    user = get_mandatory_environment_variable(POSTGRES_USER_ENV)
    password = get_mandatory_environment_variable(POSTGRES_PASSWORD_ENV)

    conn_pool = SqlConnectionPool(host=host, port=port, db=db, user=user, password=password)
    try:
        data_store = DataStore(conn_pool)
        try:
            if cli_args.create_tables_at_start:
                data_store.create_tables()
                logging.info("Created database tables")

            yield data_store
        finally:
            if cli_args.delete_tables_at_end:
                data_store.delete_tables()
                logging.info("Deleted database tables")
    finally:
        conn_pool.close()
