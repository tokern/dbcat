from dbcat.api import scan_sources


def test_pull_include_table(setup_catalog_and_data):
    catalog = setup_catalog_and_data
    scan_sources(catalog, ["sqlite_db"], include_table_regex=["full.*"])

    with catalog.managed_session:
        source = catalog.get_source("sqlite_db")
        assert source is not None

        schemata = source.schemata
        assert len(schemata) == 1

        tables = schemata[0].tables
        assert len(tables) == 1

        assert tables[0].name == "full_pii"


def test_pull_include_table_list(setup_catalog_and_data):
    catalog = setup_catalog_and_data
    scan_sources(catalog, ["sqlite_db"], include_table_regex=["full.*", "partial.*"])

    with catalog.managed_session:
        source = catalog.get_source("sqlite_db")
        assert source is not None

        schemata = source.schemata
        assert len(schemata) == 1

        tables = schemata[0].tables
        assert len(tables) == 2

        assert tables[0].name == "full_pii"
        assert tables[1].name == "partial_pii"


def test_pull_exclude_table(setup_catalog_and_data):
    catalog = setup_catalog_and_data
    scan_sources(catalog, ["pg"], exclude_table_regex=["full.*", "partial.*"])

    with catalog.managed_session:
        source = catalog.get_source("pg")
        assert source is not None

        schemata = source.schemata
        assert len(schemata) == 1

        tables = schemata[0].tables
        assert len(tables) == 1

        assert tables[0].name == "no_pii"


def test_pull_exclude_table_list(setup_catalog_and_data):
    catalog = setup_catalog_and_data
    scan_sources(catalog, ["pg"], exclude_table_regex=["full.*"])

    with catalog.managed_session:
        source = catalog.get_source("pg")
        assert source is not None

        schemata = source.schemata
        assert len(schemata) == 1

        tables = schemata[0].tables
        assert len(tables) == 2

        assert tables[0].name == "no_pii"
        assert tables[1].name == "partial_pii"
