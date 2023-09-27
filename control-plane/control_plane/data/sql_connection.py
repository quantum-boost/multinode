import psycopg2


def get_postgres_connection(
    host: str, port: int, db: str, user: str, password: str
) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=host, port=port, database=db, user=user, password=password
    )

    # The following two settings are redundant in the current version of the code because we don't have any
    # multistep transactions. These settings are included for future-proofing.

    # READ COMMITTED is actually the default in most PostgreSQL databases.
    # But we'll set this explicitly on the client side just in case as a safeguard against unexpected behaviour.
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

    # Disable autocommit to give our code full control over when commits happen.
    conn.autocommit = False

    return conn


def get_local_postgres_connection() -> psycopg2.extensions.connection:
    # For tests
    return get_postgres_connection(
        host="localhost", port=5432, db="db", user="user", password="password"
    )
