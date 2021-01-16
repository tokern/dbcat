import logging
from contextlib import closing
from typing import Any, Tuple, Type

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.base_postgres_metadata_extractor import (
    BasePostgresMetadataExtractor,
)
from databuilder.extractor.bigquery_metadata_extractor import BigQueryMetadataExtractor
from databuilder.extractor.glue_extractor import GlueExtractor
from databuilder.extractor.mysql_metadata_extractor import MysqlMetadataExtractor
from databuilder.extractor.postgres_metadata_extractor import PostgresMetadataExtractor
from databuilder.extractor.redshift_metadata_extractor import RedshiftMetadataExtractor
from databuilder.extractor.snowflake_metadata_extractor import (
    SnowflakeMetadataExtractor,
)
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_metadata import TableMetadata
from pyhocon import ConfigFactory, ConfigTree

from dbcat.catalog.metadata import Column, Connection, Database, Schema, Table
from dbcat.scanners import Scanner


class DbScanner(Scanner):
    def __init__(self, connection: Connection):
        super().__init__(connection.name)
        self._extractor: Extractor = None
        self._conf: ConfigTree = None

        if connection.type == "bigquery":
            self._extractor, self._conf = DbScanner._create_big_query_extractor(
                connection
            )
        elif connection.type == "glue":
            self._extractor, self._conf = DbScanner._create_glue_extractor(connection)
        elif connection.type == "mysql":
            self._extractor, self._conf = DbScanner._create_mysql_extractor(connection)
        elif connection.type == "postgres":
            self._extractor, self._conf = DbScanner._create_postgres_extractor(
                connection
            )
        elif connection.type == "redshift":
            self._extractor, self._conf = DbScanner._create_redshift_extractor(
                connection
            )
        elif connection.type == "snowflake":
            self._extractor, self._conf = DbScanner._create_snowflake_extractors(
                connection
            )
        else:
            raise ValueError("{} is not supported".format(connection.type))

    def scan(self) -> Database:
        with closing(self._extractor) as extractor:
            extractor.init(Scoped.get_scoped_conf(self._conf, extractor.get_scope()))

            record: TableMetadata = extractor.extract()
            current_catalog: Database = Database(record.cluster, None)
            current_schema: Schema = Schema(record.schema, current_catalog)

            while record:
                logging.debug(record)
                if record.schema != current_schema.name:
                    current_catalog.add_child(current_schema)
                    current_schema = Schema(record.schema, current_catalog)

                table: Table = Table(record.name, current_schema)
                for c in record.columns:
                    table.add_child(Column(c.name, table, c.type))

                current_schema.add_child(table)
                record = extractor.extract()
            current_catalog.add_child(current_schema)
            return current_catalog

    @staticmethod
    def _create_sqlalchemy_extractor(
        connection: Connection, extractorClass: Type[BasePostgresMetadataExtractor]
    ) -> Tuple[BasePostgresMetadataExtractor, Any]:
        extractor = extractorClass()
        scope = extractor.get_scope()
        conn_string_key = f"{scope}.{SQLAlchemyExtractor().get_scope()}.{SQLAlchemyExtractor.CONN_STRING}"

        conf = ConfigFactory.from_dict(
            {
                conn_string_key: connection.conn_string,
                f"{scope}.{extractorClass.CLUSTER_KEY}": connection.cluster,
                f"{scope}.{extractorClass.DATABASE_KEY}": connection.name,
                f"{scope}.{extractorClass.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
            }
        )

        return extractor, conf

    @staticmethod
    def _create_big_query_extractor(
        connection: Connection,
    ) -> Tuple[BigQueryMetadataExtractor, Any]:
        extractor = BigQueryMetadataExtractor()
        scope = extractor.get_scope()

        conf = ConfigFactory.from_dict(
            {
                f"{scope}.connection_name": connection.name,
                f"{scope}.key_path": connection.key_path,
                f"{scope}.project_id": connection.project_id,
                f"{scope}.project_credentials": connection.project_credentials,
                f"{scope}.page_size": connection.page_size,
                f"{scope}.filter_key": connection.filter_key,
                f"{scope}.included_tables_regex": connection.included_tables_regex,
            }
        )

        return extractor, conf

    @staticmethod
    def _create_glue_extractor(connection: Connection) -> Tuple[GlueExtractor, Any]:
        extractor = GlueExtractor()

        conf = ConfigFactory.from_dict(
            {
                f"extractor.glue.{GlueExtractor.CLUSTER_KEY}": "",  # TODO Setup Glue Config correctly
                f"extractor.glue.{GlueExtractor.FILTER_KEY}": [],
            }
        )

        return extractor, conf

    @staticmethod
    def _create_mysql_extractor(
        connection: Connection,
    ) -> Tuple[MysqlMetadataExtractor, Any]:
        connection.where_clause_suffix = """
        WHERE
            c.table_schema NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')
        """

        extractor = MysqlMetadataExtractor()
        scope = extractor.get_scope()
        conn_string_key = f"{scope}.{SQLAlchemyExtractor().get_scope()}.{SQLAlchemyExtractor.CONN_STRING}"

        conf = ConfigFactory.from_dict(
            {
                conn_string_key: connection.conn_string,
                f"{scope}.{MysqlMetadataExtractor.CLUSTER_KEY}": connection.cluster,
                f"{scope}.{MysqlMetadataExtractor.DATABASE_KEY}": connection.name,
                f"{scope}.{MysqlMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
            }
        )

        return extractor, conf

    @staticmethod
    def _create_postgres_extractor(connection: Connection) -> Tuple[Extractor, Any]:
        connection.where_clause_suffix = """
            WHERE TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog')
        """
        return DbScanner._create_sqlalchemy_extractor(
            connection, PostgresMetadataExtractor
        )

    @staticmethod
    def _create_redshift_extractor(connection: Connection) -> Tuple[Extractor, Any]:
        connection.where_clause_suffix = """
            WHERE TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog')
        """
        return DbScanner._create_sqlalchemy_extractor(
            connection, RedshiftMetadataExtractor
        )

    @staticmethod
    def _create_snowflake_extractors(
        connection: Connection,
    ) -> Tuple[SnowflakeMetadataExtractor, Any]:
        extractor = SnowflakeMetadataExtractor()
        scope = extractor.get_scope()
        conn_string_key = f"{scope}.{SQLAlchemyExtractor().get_scope()}.{SQLAlchemyExtractor.CONN_STRING}"

        conf = ConfigFactory.from_dict(
            {
                conn_string_key: connection.conn_string,
                f"{scope}.{SnowflakeMetadataExtractor.CLUSTER_KEY}": connection.cluster,
                f"{scope}.{SnowflakeMetadataExtractor.DATABASE_KEY}": connection.name,
                f"{scope}.{SnowflakeMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
            }
        )

        return extractor, conf
