import pytest

from dbcat import pull, pull_all
from dbcat.catalog.models import CatSource


@pytest.fixture(scope="module")
def setup_catalog_and_data(load_all_data, open_catalog_connection):
    catalog = open_catalog_connection
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
    yield catalog
    session = catalog.scoped_session
    [session.delete(db) for db in session.query(CatSource).all()]


def run_asserts(catalog, connection_name):
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
    pull_all(catalog)
    run_asserts(catalog, "pg")
    run_asserts(catalog, "mysql")


@pytest.mark.parametrize("source", ["pg", "mysql"])
def test_pull(setup_catalog_and_data, source):
    catalog = setup_catalog_and_data
    pull(catalog, source)
    run_asserts(catalog, source)
