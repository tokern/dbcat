from dbcat import DbScanner


def test_snowflake_extractor(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        source = catalog.add_source(
            name="sf1_name",
            source_type="snowflake",
            database="sf_db_name",
            username="sf_username",
            password="sf_password",
            account="sf_account",
            role="sf_role",
            warehouse="sf_warehouse",
        )
        extractor, conn_conf = DbScanner._create_snowflake_extractors(source)
    assert (
        conn_conf.get("{}.database_key".format(extractor.get_scope())) == "sf_db_name"
    )
    assert (
        conn_conf.get("{}.snowflake_database".format(extractor.get_scope()))
        == "sf_db_name"
    )
