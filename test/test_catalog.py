from contextlib import closing

import pytest

from dbcat.catalog.orm import CatColumn, CatDatabase, CatSchema, CatTable, Connection
from dbcat.scanners.json import File


@pytest.fixture
def load_catalog():
    scanner = File("test", "test/catalog.json")
    yield scanner.scan()


def test_file_scanner(load_catalog):
    catalog = load_catalog
    assert catalog.name == "test"
    assert len(catalog.schemata) == 1

    default_schema = catalog.schemata[0]
    assert default_schema.name == "default"
    assert len(default_schema.tables) == 8


def test_catalog_config(root_connection):
    conn: Connection = root_connection
    assert conn.type == "postgres"
    assert conn.user == "piiuser"
    assert conn.password == "p11secret"
    assert conn.host == "127.0.0.1"
    assert conn.port == 5432
    assert conn.database == "piidb"


def test_sqlalchemy_root(root_connection):
    with root_connection.engine.connect() as conn:
        conn.execute("select 1")


def test_catalog_tables(catalog_connection: Connection):
    session = catalog_connection.session
    try:
        assert len(session.query(CatDatabase).all()) == 0
        assert len(session.query(CatSchema).all()) == 0
        assert len(session.query(CatTable).all()) == 0
        assert len(session.query(CatColumn).all()) == 0
    finally:
        session.close()


@pytest.fixture
def save_catalog(load_catalog, catalog_connection):
    catalog = load_catalog
    catalog_connection.save_catalog(catalog)
    yield catalog_connection
    with closing(catalog_connection.session) as session:
        [session.delete(db) for db in session.query(CatDatabase).all()]


def test_read_catalog(save_catalog):
    connection = save_catalog

    with closing(connection.session) as session:
        dbs = session.query(CatDatabase).all()
        assert len(dbs) == 1
        db = dbs[0]
        assert db.name == "test"

        assert len(db.schemata) == 1
        schema = db.schemata[0]

        assert schema.name == "default"
        assert len(schema.tables) == 8

        tables = (
            session.query(CatTable)
            .filter(CatTable.name == "normalized_pagecounts")
            .all()
        )
        assert len(tables) == 1
        table = tables[0]
        assert table is not None
        assert table.name == "normalized_pagecounts"
        assert len(table.columns) == 5

        page_id_column = table.columns[0]
        assert page_id_column.name == "page_id"
        assert page_id_column.type == "BIGINT"
        assert page_id_column.sort_order == 0

        page_title_column = table.columns[1]
        assert page_title_column.name == "page_title"
        assert page_title_column.type == "STRING"
        assert page_title_column.sort_order == 1

        page_url_column = table.columns[2]
        assert page_url_column.name == "page_url"
        assert page_url_column.type == "STRING"
        assert page_url_column.sort_order == 2

        views_column = table.columns[3]
        assert views_column.name == "views"
        assert views_column.type == "BIGINT"
        assert views_column.sort_order == 3

        bytes_sent_column = table.columns[4]
        assert bytes_sent_column.name == "bytes_sent"
        assert bytes_sent_column.type == "BIGINT"
        assert bytes_sent_column.sort_order == 4
