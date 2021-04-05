from contextlib import closing

import pytest
import yaml

from dbcat.catalog.catalog import Catalog
from dbcat.catalog.models import (
    CatColumn,
    CatSchema,
    CatSource,
    CatTable,
    ColumnLineage,
)


class File:
    def __init__(self, name: str, path: str, catalog: Catalog):
        self.name = name
        self._path = path
        self._catalog = catalog

    @property
    def path(self):
        return self._path

    def scan(self):
        import json

        with open(self.path, "r") as file:
            content = json.load(file)

        with self._catalog:
            source = self._catalog.add_source(
                name=content["name"], type=content["type"]
            )
            for s in content["schemata"]:
                schema = self._catalog.add_schema(s["name"], source=source)

                for t in s["tables"]:
                    table = self._catalog.add_table(t["name"], schema)

                    index = 0
                    for c in t["columns"]:
                        self._catalog.add_column(
                            column_name=c["name"],
                            type=c["type"],
                            sort_order=index,
                            table=table,
                        )
                        index += 1


@pytest.fixture(scope="module")
def save_catalog(open_catalog_connection):
    catalog = open_catalog_connection
    scanner = File("test", "test/catalog.json", catalog)
    scanner.scan()
    yield catalog
    with closing(catalog.session) as session:
        [session.delete(db) for db in session.query(CatSource).all()]
        session.commit()


def test_catalog_config(root_connection):
    conn: Catalog = root_connection
    assert conn.type == "postgres"
    assert conn.user == "piiuser"
    assert conn.password == "p11secret"
    assert conn.host == "127.0.0.1"
    assert conn.port == 5432
    assert conn.database == "piidb"


def test_sqlalchemy_root(root_connection):
    with root_connection.engine.connect() as conn:
        conn.execute("select 1")


def test_catalog_tables(open_catalog_connection: Catalog):
    session = open_catalog_connection.session
    try:
        assert len(session.query(CatSource).all()) == 0
        assert len(session.query(CatSchema).all()) == 0
        assert len(session.query(CatTable).all()) == 0
        assert len(session.query(CatColumn).all()) == 0
    finally:
        session.close()


def test_read_catalog(save_catalog):
    catalog = save_catalog

    with closing(catalog.session) as session:
        dbs = session.query(CatSource).all()
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


@pytest.mark.skip
def test_update_catalog(save_catalog):
    catalog = save_catalog

    with closing(catalog.session) as session:
        page_counts = (
            session.query(CatTable).filter(CatTable.name == "pagecounts").one()
        )
        group_col = (
            session.query(CatColumn)
            .filter(CatColumn.name == "group", CatColumn.table == page_counts)
            .one()
        )

        assert group_col.type == "STRING"

    catalog.schemata[0].tables[0].columns[0]._type = "BIGINT"
    catalog.save_catalog(catalog)

    with closing(catalog.session) as session:
        page_counts = (
            session.query(CatTable).filter(CatTable.name == "pagecounts").one()
        )
        group_col = (
            session.query(CatColumn)
            .filter(CatColumn.name == "group", CatColumn.table == page_counts)
            .one()
        )

        assert group_col.type == "BIGINT"


def test_get_database(save_catalog):
    catalog = save_catalog
    database = catalog.get_source("test")
    assert database.fqdn == "test"


def test_get_schema(save_catalog):
    catalog = save_catalog
    schema = catalog.get_schema("test", "default")
    assert schema.fqdn == ("test", "default")


def test_get_table(save_catalog):
    catalog = save_catalog
    table = catalog.get_table("test", "default", "page")
    assert table.fqdn == ("test", "default", "page")


def test_get_table_columns(save_catalog):
    catalog = save_catalog
    table = catalog.get_table("test", "default", "page")
    columns = catalog.get_columns_for_table(table)
    assert len(columns) == 3


def test_get_column_in(save_catalog):
    catalog = save_catalog
    table = catalog.get_table("test", "default", "page")
    columns = catalog.get_columns_for_table(
        table=table, column_names=["page_id", "page_latest"]
    )
    assert len(columns) == 2

    columns = catalog.get_columns_for_table(table=table, column_names=["page_id"])
    assert len(columns) == 1


def test_get_column(save_catalog):
    catalog = save_catalog
    column = catalog.get_column("test", "default", "page", "page_title")
    assert column.fqdn == ("test", "default", "page", "page_title")


def test_search_source(save_catalog):
    catalog = save_catalog
    databases = catalog.search_sources("t%")
    assert len(databases) == 1


def test_search_schema(save_catalog):
    catalog = save_catalog
    schemata = catalog.search_schema(source_like="test", schema_like="def%")
    assert len(schemata) == 1

    name_only = catalog.search_schema(schema_like="def%")
    assert len(name_only) == 1


def test_search_tables(save_catalog):
    catalog = save_catalog
    tables = catalog.search_tables(
        source_like="test", schema_like="default", table_like="page%"
    )
    assert len(tables) == 5

    name_only = catalog.search_tables(table_like="page%")
    assert len(name_only) == 5


def test_search_table(save_catalog):
    catalog = save_catalog
    table = catalog.search_table(
        source_like="test", schema_like="default", table_like="pagecount%"
    )
    assert table is not None

    name_only = catalog.search_table(table_like="pagecount%")
    assert name_only is not None


def test_search_table_not_found(save_catalog):
    catalog = save_catalog
    with pytest.raises(RuntimeError):
        catalog.search_table(
            source_like="test", schema_like="default", table_like="blah"
        )


#    assert e.str() == "'blah' table not found"


def test_search_table_multiple(save_catalog):
    catalog = save_catalog
    with pytest.raises(RuntimeError):
        catalog.search_table(
            source_like="test", schema_like="default", table_like="page%"
        )
        # assert e == "Ambiguous table name. Multiple matches found"


def test_search_column(save_catalog):
    catalog = save_catalog
    columns = catalog.search_column(
        source_like="test",
        schema_like="default",
        table_like="pagecounts",
        column_like="views",
    )
    assert len(columns) == 1

    name_only = catalog.search_column(column_like="view%")
    assert len(name_only) == 3


def test_add_sources(open_catalog_connection):
    catalog = open_catalog_connection
    with open("test/connections.yaml") as f:
        connections = yaml.safe_load(f)

    with catalog:
        for c in connections["connections"]:
            print(c)
            catalog.add_source(**c)

    connections = catalog.search_sources(source_like="%")
    assert len(connections) == 6

    # pg
    pg_connection = connections[1]
    assert pg_connection.name == "pg"
    assert pg_connection.type == "postgres"
    assert pg_connection.database == "db_database"
    assert pg_connection.username == "db_user"
    assert pg_connection.password == "db_password"
    assert pg_connection.port == "db_port"
    assert pg_connection.uri == "db_uri"

    # mysql
    mysql_conn = connections[2]
    assert mysql_conn.name == "mys"
    assert mysql_conn.type == "mysql"
    assert mysql_conn.database == "db_database"
    assert mysql_conn.username == "db_user"
    assert mysql_conn.password == "db_password"
    assert mysql_conn.port == "db_port"
    assert mysql_conn.uri == "db_uri"

    # bigquery
    bq_conn = connections[3]
    assert bq_conn.name == "bq"
    assert bq_conn.type == "bigquery"
    assert bq_conn.key_path == "db_key_path"
    assert bq_conn.project_credentials == "db_creds"
    assert bq_conn.project_id == "db_project_id"

    # glue
    glue_conn = connections[4]
    assert glue_conn.name == "gl"
    assert glue_conn.type == "glue"

    # snowflake
    sf_conn = connections[5]
    assert sf_conn.name == "sf"
    assert sf_conn.type == "snowflake"
    assert sf_conn.database == "db_database"
    assert sf_conn.username == "db_user"
    assert sf_conn.password == "db_password"
    assert sf_conn.account == "db_account"
    assert sf_conn.role == "db_role"
    assert sf_conn.warehouse == "db_warehouse"


def load_edges(catalog, expected_edges, job_id):
    column_edge_ids = []
    with catalog:
        for edge in expected_edges:
            source = catalog.get_column(
                database_name=edge[0][0],
                schema_name=edge[0][1],
                table_name=edge[0][2],
                column_name=edge[0][3],
            )

            target = catalog.get_column(
                database_name=edge[1][0],
                schema_name=edge[1][1],
                table_name=edge[1][2],
                column_name=edge[1][3],
            )

            added_edge = catalog.add_column_lineage(source, target, job_id, {})

            column_edge_ids.append(added_edge.id)
    return column_edge_ids


@pytest.fixture(scope="module")
def load_page_lookup_nonredirect_edges(save_catalog):
    catalog = save_catalog
    expected_edges = [
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "redirect_id"),
        ),
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "redirect_title"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "true_title"),
        ),
        (
            ("test", "default", "page", "page_latest"),
            ("test", "default", "page_lookup_nonredirect", "page_version"),
        ),
    ]

    column_edge_ids = load_edges(
        catalog, expected_edges, "insert_page_lookup_nonredirect"
    )
    yield catalog, expected_edges

    with closing(catalog.session) as session:
        session.query(ColumnLineage).filter(
            ColumnLineage.id.in_(column_edge_ids)
        ).delete(synchronize_session=False)


@pytest.fixture(scope="module")
def insert_page_lookup_redirect(save_catalog):
    catalog = save_catalog
    expected_edges = [
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_redirect", "page_id"),
        ),
        (
            ("test", "default", "page", "page_latest"),
            ("test", "default", "page_lookup_redirect", "page_version"),
        ),
    ]

    column_edge_ids = load_edges(catalog, expected_edges, "insert_page_lookup_redirect")
    yield catalog, expected_edges

    with closing(catalog.session) as session:
        session.query(ColumnLineage).filter(
            ColumnLineage.id.in_(column_edge_ids)
        ).delete(synchronize_session=False)


def test_add_edge(load_page_lookup_nonredirect_edges):
    catalog, expected_edges = load_page_lookup_nonredirect_edges
    with closing(catalog.session) as session:
        all_edges = session.query(ColumnLineage).all()
        assert set([(e.source.fqdn, e.target.fqdn) for e in all_edges]) == set(
            expected_edges
        )


def test_get_all_edges(load_page_lookup_nonredirect_edges, insert_page_lookup_redirect):
    catalog, expected_nonredirect = load_page_lookup_nonredirect_edges

    edges = catalog.get_column_lineages()
    assert len(edges) == 7


def test_get_edges_for_job(
    load_page_lookup_nonredirect_edges, insert_page_lookup_redirect
):
    catalog, expected_nonredirect = load_page_lookup_nonredirect_edges

    edges = catalog.get_column_lineages(job_ids=["insert_page_lookup_redirect"])
    assert len(edges) == 2


def test_get_edges_for_many_jobs(
    load_page_lookup_nonredirect_edges, insert_page_lookup_redirect
):
    catalog, expected_nonredirect = load_page_lookup_nonredirect_edges

    edges = catalog.get_column_lineages(
        job_ids=["insert_page_lookup_redirect", "insert_page_lookup_nonredirect"]
    )
    assert len(edges) == 7
