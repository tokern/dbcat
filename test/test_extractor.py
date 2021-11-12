from contextlib import closing
from typing import List

from databuilder import Scoped
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_metadata import TableMetadata
from pyhocon import ConfigFactory

from dbcat.catalog.db import DbScanner
from dbcat.catalog.sqlite_extractor import SqliteMetadataExtractor


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
        extractor, conn_conf = DbScanner._create_snowflake_extractor(source)
    assert (
        conn_conf.get("{}.database_key".format(extractor.get_scope())) == "sf_db_name"
    )
    assert (
        conn_conf.get("{}.snowflake_database".format(extractor.get_scope()))
        == "sf_db_name"
    )


def test_sqlite_extractor(load_all_data):
    params, path = load_all_data
    conn_string_key = f"{SqliteMetadataExtractor().get_scope()}.{SQLAlchemyExtractor().get_scope()}.{SQLAlchemyExtractor.CONN_STRING}"
    config = ConfigFactory.from_dict(
        {conn_string_key: "sqlite:///{path}".format(path=path)}
    )
    with closing(SqliteMetadataExtractor()) as extractor:
        extractor.init(Scoped.get_scoped_conf(config, extractor.get_scope()))

        records: List[TableMetadata] = []

        while record := extractor.extract():
            records.append(record)

        assert len(records) == 3
        full_pii_table = records[0]
        assert full_pii_table.name == "full_pii"
        assert len(full_pii_table.columns) == 2
        assert full_pii_table.columns[0].name == "location"
        assert full_pii_table.columns[1].name == "name"

        no_pii_table = records[1]
        assert no_pii_table.name == "no_pii"
        assert len(no_pii_table.columns) == 2
        assert no_pii_table.columns[0].name == "a"
        assert no_pii_table.columns[1].name == "b"

        partial_pii_table = records[2]
        assert partial_pii_table.name == "partial_pii"
        assert len(partial_pii_table.columns) == 2
        assert partial_pii_table.columns[0].name == "a"
        assert partial_pii_table.columns[1].name == "b"
