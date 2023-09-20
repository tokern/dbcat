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

def test_bigquery_extractor(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        source = catalog.add_source(
            name="bq1_source",
            source_type="bigquery",
            username="bq_username",
            key_path="bq_keypath",
            cred_key="bq_keypath",
            project_id="bq_project_id"
        )
        extractor, conn_conf = DbScanner._create_big_query_extractor(source)
    assert (
        conn_conf.get("{}.key_path".format(extractor.get_scope())) == "bq_keypath"
    )
    


def test_athena_extractor(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        source = catalog.add_source(
            name="athena_name",
            source_type="athena",
            aws_access_key_id="access_key",
            aws_secret_access_key="secret_key",
            region_name="us_east_1",
            s3_staging_dir="staging_dir",
            mfa="mfa",
            aws_session_token="aws_session_token",
        )

        extractor, conn_conf = DbScanner._create_athena_extractor(source)
        scoped = Scoped.get_scoped_conf(
            Scoped.get_scoped_conf(conn_conf, extractor.get_scope()),
            SQLAlchemyExtractor().get_scope(),
        )
        assert (
            scoped.get_string(SQLAlchemyExtractor.CONN_STRING)
            == 'awsathena+rest://access_key:secret_key@athena.us_east_1.amazonaws.com:443/?s3_staging_dir=staging_dir&aws_session_token=aws_session_token&mfa_serial=mfa'
        )


def test_athena_extractor_iam(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        source = catalog.add_source(
            name="athena_iam",
            source_type="athena",
            region_name="us_east_1",
            s3_staging_dir="staging_dir",
            mfa="mfa",
            aws_session_token="aws_session_token",
        )

        extractor, conn_conf = DbScanner._create_athena_extractor(source)
        scoped = Scoped.get_scoped_conf(
            Scoped.get_scoped_conf(conn_conf, extractor.get_scope()),
            SQLAlchemyExtractor().get_scope(),
        )
        assert (
            scoped.get_string(SQLAlchemyExtractor.CONN_STRING)
            == 'awsathena+rest://:@athena.us_east_1.amazonaws.com:443/?s3_staging_dir=staging_dir&aws_session_token=aws_session_token&mfa_serial=mfa'
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

        record = extractor.extract()
        while record:
            records.append(record)
            record = extractor.extract()

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

def test_oracle_extractor(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        source = catalog.add_source(
            catalog=catalog,
            name="oracle_db",
            uri="db_uri",
            username="db_user",
            password="db_password",
            database="db_database",
            port="db_port",
        )

        extractor, conn_conf = DbScanner._create_oracle_extractor(source)
        scoped = Scoped.get_scoped_conf(
            Scoped.get_scoped_conf(conn_conf, extractor.get_scope()),
            SQLAlchemyExtractor().get_scope(),
        )
        assert (
            scoped.get_string(SQLAlchemyExtractor.CONN_STRING)
            == f"oracle+cx_oracle://db_user:db_password@db_uri:db_port/db_database"
        )