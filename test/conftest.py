import logging
import os
import sqlite3
from contextlib import closing
from shutil import rmtree
from typing import Generator, Tuple

import psycopg2
import pymysql
import pytest
import yaml
from pytest_cases import fixture, parametrize_with_cases

from dbcat import catalog_connection_yaml, init_db
from dbcat.catalog import CatSource
from dbcat.catalog.catalog import Catalog, PGCatalog

postgres_conf = """
catalog:
  user: piiuser
  password: p11secret
  host: 127.0.0.1
  port: 5432
  database: piidb
"""


@pytest.fixture(scope="module")
def root_connection():
    config = yaml.safe_load(postgres_conf)
    with closing(PGCatalog(**config["catalog"])) as conn:
        yield conn


sqlite_catalog_conf = """
catalog:
  path: {path}
"""


@fixture(scope="module")
def temp_sqlite_path(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("sqlite_test")
    sqlite_path = temp_dir.join("sqldb")

    yield sqlite_path

    os.remove(sqlite_path)


def case_setup_sqlite(temp_sqlite_path):
    return sqlite_catalog_conf.format(path=temp_sqlite_path)


pg_catalog_conf = """
catalog:
  user: catalog_user
  password: catal0g_passw0rd
  host: 127.0.0.1
  port: 5432
  database: tokern
"""


@fixture(scope="module")
def setup_pg_catalog():
    config = yaml.safe_load(postgres_conf)
    with closing(PGCatalog(**config["catalog"])) as root_connection:
        with root_connection.engine.connect() as conn:
            conn.execute("CREATE USER catalog_user PASSWORD 'catal0g_passw0rd'")
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "CREATE DATABASE tokern"
            )
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "GRANT ALL PRIVILEGES ON DATABASE tokern TO catalog_user"
            )

        yield

        with root_connection.engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "DROP DATABASE tokern"
            )

            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "DROP USER catalog_user"
            )


def case_setup_pg(setup_pg_catalog):
    return pg_catalog_conf


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

    logging.info("Sqlite temp db {}", str(sqlite_path))

    yield str(sqlite_path)

    rmtree(temp_dir)
    logging.info("Deleted {}", str(temp_dir))


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


def sqlite_conn(path: str):
    return sqlite3.connect(path), path


@pytest.fixture(scope="module")
def load_all_data(temp_sqlite_db):
    logging.info("Load data")
    params = [mysql_conn(), pg_conn()]
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
