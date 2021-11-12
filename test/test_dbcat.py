import pytest

from dbcat.api import scan_sources


def run_asserts(catalog, connection_name):
    with catalog.managed_session:
        pg_source = catalog.get_source(connection_name)
        assert pg_source is not None

        pg_schemata = pg_source.schemata
        assert len(pg_schemata) == 1

        pg_tables = pg_schemata[0].tables
        assert len(pg_tables) == 3

        pg_columns = []
        for table in pg_tables:
            pg_columns = pg_columns + table.columns
        assert len(pg_columns) == 6


def test_pull_all(setup_catalog_and_data):
    catalog = setup_catalog_and_data
    scan_sources(catalog, ["pg", "mysql", "sqlite_db"])
    run_asserts(catalog, "pg")
    run_asserts(catalog, "mysql")
    run_asserts(catalog, "sqlite_db")


@pytest.mark.parametrize("source", ["pg", "mysql", "sqlite_db"])
def test_pull(setup_catalog_and_data, source):
    catalog = setup_catalog_and_data
    scan_sources(catalog, [source])
    run_asserts(catalog, source)
