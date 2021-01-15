from contextlib import closing

import psycopg2
import pymysql
import pytest

from dbcat.catalog.metadata import Connection, Schema, Table
from dbcat.scanners.db import DbSchema

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
        Connection(
            metadata_source="mysql",
            uri="127.0.0.1",
            username="piiuser",
            password="p11secret",
            database="piidb",
        ),
    )


def pg_conn():
    return (
        psycopg2.connect(
            host="127.0.0.1", user="piiuser", password="p11secret", database="piidb"
        ),
        Connection(
            metadata_source="postgres",
            uri="127.0.0.1",
            username="piiuser",
            password="p11secret",
            database="piidb",
            cluster="public",
        ),
    )


@pytest.fixture(params=[mysql_conn(), pg_conn()])
def load_data(request):
    db_conn, extractor_conn = request.param
    with closing(db_conn) as conn:
        with conn.cursor() as cursor:
            for statement in pii_data_load:
                cursor.execute(statement)
            cursor.execute("commit")
        yield conn, extractor_conn
        with conn.cursor() as cursor:
            for statement in pii_data_drop:
                cursor.execute(statement)
            cursor.execute("commit")


def test_catalog(load_data):
    db_conn, extractor_conn = load_data

    scanner: DbSchema = DbSchema(connection=extractor_conn)
    catalog = scanner.scan()
    assert catalog.name == "piidb"
    assert len(catalog.schemata) == 1

    schema: Schema = catalog.schemata[0]
    assert schema.name == "public"
    assert len(schema.tables) == 3

    full_pii: Table = schema.tables[0]
    no_pii: Table = schema.tables[1]
    partial_pii: Table = schema.tables[2]

    assert full_pii.name == "full_pii"
    assert no_pii.name == "no_pii"
    assert partial_pii.name == "partial_pii"

    assert full_pii.columns[0].name == "name"
    assert full_pii.columns[0].type == "text"
    assert full_pii.columns[1].name == "location"
    assert full_pii.columns[1].type == "text"
