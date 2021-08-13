from contextlib import closing

import psycopg2
import pymysql
import pytest
import yaml
from pytest_lazyfixture import lazy_fixture

from dbcat import catalog_connection, init_db
from dbcat.catalog.catalog import PGCatalog

postgres_conf = """
catalog:
  user: piiuser
  password: p11secret
  host: 127.0.0.1
  port: 5432
  database: piidb
"""


@pytest.fixture(scope="session")
def root_connection():
    config = yaml.safe_load(postgres_conf)
    with closing(PGCatalog(**config["catalog"])) as conn:
        yield conn


sqlite_catalog_conf = """
catalog:
  path: {path}
"""


@pytest.fixture(scope="session")
def setup_sqlite(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("sqlite_test")
    sqlite_path = temp_dir.join("sqldb")

    yield None, sqlite_catalog_conf.format(path=sqlite_path)


pg_catalog_conf = """
catalog:
  user: catalog_user
  password: catal0g_passw0rd
  host: 127.0.0.1
  port: 5432
  database: tokern
"""


@pytest.fixture(scope="session")
def setup_pg(root_connection):
    with root_connection.engine.connect() as conn:
        conn.execute("CREATE USER catalog_user PASSWORD 'catal0g_passw0rd'")
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "CREATE DATABASE tokern"
        )
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "GRANT ALL PRIVILEGES ON DATABASE tokern TO catalog_user"
        )

    yield root_connection, pg_catalog_conf

    with root_connection.engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "DROP DATABASE tokern"
        )

        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "DROP USER catalog_user"
        )


@pytest.fixture(
    scope="session", params=[lazy_fixture("setup_sqlite"), lazy_fixture("setup_pg")]
)
def setup_catalog(request):
    yield request.param


@pytest.fixture(scope="session")
def open_catalog_connection(setup_catalog):
    connection, conf = setup_catalog
    with closing(catalog_connection(conf)) as conn:
        init_db(conn)
        yield conn, conf


pii_data_script = """
create table no_pii(a text, b text);
insert into no_pii values ('abc', 'def');
insert into no_pii values ('xsfr', 'asawe');

create table partial_pii(a text, b text);
insert into partial_pii values ('917-908-2234', 'plkj');
insert into partial_pii values ('215-099-2234', 'sfrf');

create table full_pii(name text, location text);
insert into full_pii values ('Jonathan Smith', 'Virginia');
insert into full_pii values ('Chase Ryan', 'Chennai');

"""


pii_data_load = [
    "create table no_pii(a text, b text)",
    "insert into no_pii values ('abc', 'def')",
    "insert into no_pii values ('xsfr', 'asawe')",
    "create table partial_pii(a text, b text)",
    "insert into partial_pii values ('917-908-2234', 'plkj')",
    "insert into partial_pii values ('215-099-2234', 'sfrf')",
    "create table full_pii(name text, location text)",
    "insert into full_pii values ('Jonathan Smith', 'Virginia')",
    "insert into full_pii values ('Chase Ryan', 'Chennai')",
]

pii_data_drop = ["DROP TABLE full_pii", "DROP TABLE partial_pii", "DROP TABLE no_pii"]


def mysql_conn():
    return (
        pymysql.connect(
            host="127.0.0.1", user="piiuser", password="p11secret", database="piidb",
        ),
        "piidb",
    )


def pg_conn():
    return (
        psycopg2.connect(
            host="127.0.0.1", user="piiuser", password="p11secret", database="piidb"
        ),
        "public",
    )


@pytest.fixture(params=[mysql_conn(), pg_conn()])
def load_data(request):
    db_conn, expected_schema = request.param
    with closing(db_conn) as conn:
        with conn.cursor() as cursor:
            for statement in pii_data_load:
                cursor.execute(statement)
            cursor.execute("commit")
        yield conn, expected_schema
        with conn.cursor() as cursor:
            for statement in pii_data_drop:
                cursor.execute(statement)
            cursor.execute("commit")


@pytest.fixture(scope="session")
def load_all_data():
    params = [mysql_conn(), pg_conn()]
    for p in params:
        db_conn, expected_schema = p
        with db_conn.cursor() as cursor:
            for statement in pii_data_load:
                cursor.execute(statement)
            cursor.execute("commit")
    yield params
    for p in params:
        db_conn, expected_schema = p
        with db_conn.cursor() as cursor:
            for statement in pii_data_drop:
                cursor.execute(statement)
            cursor.execute("commit")

    for p in params:
        db_conn, expected_schema = p
        db_conn.close()
