from contextlib import closing

import pytest

from dbcat import pull
from dbcat.catalog.orm import CatSource


@pytest.fixture(scope="module")
def setup_catalog_and_data(load_all_data, open_catalog_connection):
    catalog = open_catalog_connection
    with catalog:
        catalog.add_source(
            name="mysql",
            type="mysql",
            uri="127.0.0.1",
            username="piiuser",
            password="p11secret",
            database="piidb",
        )
        catalog.add_source(
            name="pg",
            type="postgres",
            uri="127.0.0.1",
            username="piiuser",
            password="p11secret",
            database="piidb",
            cluster="public",
        )
    yield catalog
    with closing(catalog.session) as session:
        [session.delete(db) for db in session.query(CatSource).all()]


def test_api(setup_catalog_and_data):
    catalog = setup_catalog_and_data
    pull(catalog)
