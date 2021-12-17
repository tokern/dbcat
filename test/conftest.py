import logging
import os
import sqlite3
from contextlib import closing
from shutil import rmtree
from typing import Any, Generator, Tuple

import psycopg2
import pymysql
import pytest
from pytest_cases import fixture, parametrize_with_cases
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound

from dbcat import settings
from dbcat.api import catalog_connection_yaml, init_db, scan_sources
from dbcat.catalog import CatSource
from dbcat.catalog.catalog import Catalog

postgres_conf = """
catalog:
  user: piiuser
  password: p11secret
  host: {host}
  port: 5432
  database: piidb
  secret: {secret}
"""


@pytest.fixture(scope="module")
def root_connection(request):
    conf = postgres_conf.format(
        host=request.config.getoption("--pg-host"),
        secret=settings.DEFAULT_CATALOG_SECRET,
    )
    with closing(catalog_connection_yaml(conf)) as conn:
        yield conn


sqlite_catalog_conf = """
catalog:
  path: {path}
  secret: {secret}
"""


@fixture(scope="module")
def temp_sqlite_path(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("sqlite_test")
    sqlite_path = temp_dir.join("sqldb")

    yield sqlite_path

    os.remove(sqlite_path)


def case_setup_sqlite(temp_sqlite_path):
    return sqlite_catalog_conf.format(
        path=temp_sqlite_path, secret=settings.DEFAULT_CATALOG_SECRET
    )


pg_catalog_conf = """
catalog:
  user: catalog_user
  password: catal0g_passw0rd
  host: {host}
  database: tokern
  secret: {secret}
"""


def pytest_addoption(parser):
    parser.addoption(
        "--pg-host", action="store", default="127.0.0.1", help="specify IP of pg host"
    )
    parser.addoption(
        "--mysql-host",
        action="store",
        default="127.0.0.1",
        help="specify IP of mysql host",
    )


@fixture(scope="module")
def setup_pg_catalog(request):
    conf = postgres_conf.format(
        host=request.config.getoption("--pg-host"),
        secret=settings.DEFAULT_CATALOG_SECRET,
    )
    with closing(catalog_connection_yaml(conf)) as root_connection:
        with root_connection.engine.connect() as conn:
            conn.execute("CREATE USER catalog_user PASSWORD 'catal0g_passw0rd'")
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "CREATE DATABASE tokern"
            )
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "GRANT ALL PRIVILEGES ON DATABASE tokern TO catalog_user"
            )

        yield pg_catalog_conf.format(
            host=request.config.getoption("--pg-host"),
            secret=settings.DEFAULT_CATALOG_SECRET,
        )

        with root_connection.engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "DROP DATABASE tokern"
            )

            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "DROP USER catalog_user"
            )


def case_setup_pg(setup_pg_catalog):
    return setup_pg_catalog


@fixture(scope="module")
@parametrize_with_cases("catalog_conf", cases=".", scope="module")
def open_catalog_connection(catalog_conf) -> Generator[Tuple[Catalog, str], None, None]:
    with closing(catalog_connection_yaml(catalog_conf)) as conn:
        init_db(conn)
        yield conn, catalog_conf


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


@pytest.fixture(scope="module")
def temp_sqlite_db(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("sqlite_extractor")
    sqlite_path = temp_dir.join("sqldb")

    logging.info("Sqlite temp db {}".format(str(sqlite_path)))

    yield str(sqlite_path)

    rmtree(temp_dir)
    logging.info("Deleted {}".format(str(temp_dir)))


def mysql_conn(host):
    return (
        pymysql.connect(
            host=host, user="piiuser", password="p11secret", database="piidb",
        ),
        "piidb",
    )


def pg_conn(host):
    return (
        psycopg2.connect(
            host=host, user="piiuser", password="p11secret", database="piidb"
        ),
        "public",
    )


def sqlite_conn(path: str):
    return sqlite3.connect(path), path


@pytest.fixture(scope="module")
def load_all_data(temp_sqlite_db, request):
    logging.info("Load data")
    params = [
        mysql_conn(request.config.getoption("--mysql-host")),
        pg_conn(request.config.getoption("--pg-host")),
    ]
    for p in params:
        db_conn, expected_schema = p
        with db_conn.cursor() as cursor:
            for statement in pii_data_load:
                cursor.execute(statement)
            cursor.execute("commit")

    with closing(sqlite3.connect(temp_sqlite_db)) as conn:
        for statement in pii_data_load:
            conn.execute(statement)
        conn.commit()

    yield params, temp_sqlite_db
    for p in params:
        db_conn, expected_schema = p
        with db_conn.cursor() as cursor:
            for statement in pii_data_drop:
                cursor.execute(statement)
            cursor.execute("commit")

    for p in params:
        db_conn, expected_schema = p
        db_conn.close()


@pytest.fixture(scope="module")
def setup_catalog_and_data(load_all_data, open_catalog_connection):
    params, sqlite_path = load_all_data
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        catalog.add_source(
            name="mysql",
            source_type="mysql",
            uri="127.0.0.1",
            username="piiuser",
            password="p11secret",
            database="piidb",
        )
        catalog.add_source(
            name="pg",
            source_type="postgresql",
            uri="127.0.0.1",
            username="piiuser",
            password="p11secret",
            database="piidb",
            cluster="public",
        )
        catalog.add_source(name="sqlite_db", source_type="sqlite", uri=sqlite_path)
    yield catalog
    with catalog.managed_session as session:
        [session.delete(db) for db in session.query(CatSource).all()]


def source_pg(open_catalog_connection, request) -> Tuple[Catalog, str, int]:
    catalog, conf = open_catalog_connection
    host = request.config.getoption("--pg-host")
    with catalog.managed_session:
        try:
            source = catalog.get_source("pg_src")
        except NoResultFound:
            source = catalog.add_source(
                name="pg_src",
                source_type="postgresql",
                uri=host,
                username="piiuser",
                password="p11secret",
                database="piidb",
                cluster="public",
            )
        source_id = source.id

    return catalog, conf, source_id


@pytest.fixture(scope="module")
def temp_sqlite(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("sqlite_test")
    sqlite_path = temp_dir / "sqldb"

    yield sqlite_path

    rmtree(temp_dir)
    logging.info("Deleted {}".format(str(temp_dir)))


def source_sqlite(
    open_catalog_connection, temp_sqlite
) -> Generator[Tuple[Catalog, str, int], None, None]:
    catalog, conf = open_catalog_connection
    sqlite_path = temp_sqlite
    with catalog.managed_session:
        try:
            source = catalog.get_source("sqlite_src")
        except NoResultFound:
            source = catalog.add_source(
                name="sqlite_src", source_type="sqlite", uri=str(sqlite_path)
            )
        yield catalog, conf, source.id


@fixture(scope="module")
@parametrize_with_cases("source", scope="module", cases=".", prefix="source_")
def create_source_engine(
    source,
) -> Generator[Tuple[Catalog, str, int, Any, str, str], None, None]:
    catalog, conf, source_id = source
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        name = source.name
        conn_string = source.conn_string
        source_type = source.source_type

    engine = create_engine(conn_string)
    yield catalog, conf, source_id, engine, name, source_type
    engine.dispose()


@pytest.fixture(scope="module")
def load_data(
    create_source_engine,
) -> Generator[Tuple[Catalog, str, int, str], None, None]:
    catalog, conf, source_id, engine, name, source_type = create_source_engine
    with engine.begin() as conn:
        for statement in pii_data_load:
            conn.execute(statement)
        if source_type != "sqlite":
            conn.execute("commit")

    yield catalog, conf, source_id, name

    with engine.begin() as conn:
        for statement in pii_data_drop:
            conn.execute(statement)
        if source_type != "sqlite":
            conn.execute("commit")


@pytest.fixture(scope="module")
def load_data_and_pull(load_data) -> Generator[Tuple[Catalog, str, int], None, None]:
    catalog, conf, source_id, name = load_data
    scan_sources(catalog, [name])
    yield catalog, conf, source_id


@pytest.fixture(scope="module")
def load_source(
    load_data_and_pull,
) -> Generator[Tuple[Catalog, str, CatSource], None, None]:
    catalog, conf, source_id = load_data_and_pull

    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        yield catalog, conf, source
