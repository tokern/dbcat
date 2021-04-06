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

from dbcat.catalog.catalog import Catalog
from dbcat.catalog.models import CatSource
from dbcat.log_mixin import LogMixin


class DbScanner(LogMixin):
    def __init__(self, catalog: Catalog, source: CatSource):
        self._name = source.name
        self._extractor: Extractor = None
        self._conf: ConfigTree = None
        self._catalog = catalog
        self._source = source
        if source.type == "bigquery":
            self._extractor, self._conf = DbScanner._create_big_query_extractor(source)
        elif source.type == "glue":
            self._extractor, self._conf = DbScanner._create_glue_extractor(source)
        elif source.type == "mysql":
            self._extractor, self._conf = DbScanner._create_mysql_extractor(source)
        elif source.type == "postgres":
            self._extractor, self._conf = DbScanner._create_postgres_extractor(source)
        elif source.type == "redshift":
            self._extractor, self._conf = DbScanner._create_redshift_extractor(source)
        elif source.type == "snowflake":
            self._extractor, self._conf = DbScanner._create_snowflake_extractors(source)
        else:
            raise ValueError("{} is not supported".format(source.type))

    @property
    def name(self):
        return self._name

    def scan(self):
        with closing(self._extractor) as extractor:
            extractor.init(Scoped.get_scoped_conf(self._conf, extractor.get_scope()))

            record: TableMetadata = extractor.extract()
            current_schema = self._catalog.add_schema(
                schema_name=record.schema, source=self._source
            )

            while record:
                logging.debug(record)
                if record.schema != current_schema.name:
                    current_schema = self._catalog.add_schema(
                        schema_name=record.schema, source=self._source
                    )

                table = self._catalog.add_table(
                    table_name=record.name, schema=current_schema
                )
                index = 0
                for c in record.columns:
                    self._catalog.add_column(
                        column_name=c.name, type=c.type, sort_order=index, table=table,
                    )
                    index += 1

                record = extractor.extract()

    @staticmethod
    def _create_sqlalchemy_extractor(
        source: CatSource,
        where_clause_suffix: str,
        extractorClass: Type[BasePostgresMetadataExtractor],
    ) -> Tuple[BasePostgresMetadataExtractor, Any]:
        extractor = extractorClass()
        scope = extractor.get_scope()
        conn_string_key = f"{scope}.{SQLAlchemyExtractor().get_scope()}.{SQLAlchemyExtractor.CONN_STRING}"

        conf = ConfigFactory.from_dict(
            {
                conn_string_key: source.conn_string,
                f"{scope}.{extractorClass.CLUSTER_KEY}": source.cluster,
                f"{scope}.{extractorClass.DATABASE_KEY}": source.name,
                f"{scope}.{extractorClass.WHERE_CLAUSE_SUFFIX_KEY}": where_clause_suffix,
            }
        )

        return extractor, conf

    @staticmethod
    def _create_big_query_extractor(
        source: CatSource,
    ) -> Tuple[BigQueryMetadataExtractor, Any]:
        extractor = BigQueryMetadataExtractor()
        scope = extractor.get_scope()

        conf = ConfigFactory.from_dict(
            {
                f"{scope}.connection_name": source.name,
                f"{scope}.key_path": source.key_path,
                f"{scope}.project_id": source.project_id,
                f"{scope}.project_credentials": source.project_credentials,
                f"{scope}.page_size": source.page_size,
                f"{scope}.filter_key": source.filter_key,
                f"{scope}.included_tables_regex": source.included_tables_regex,
            }
        )

        return extractor, conf

    @staticmethod
    def _create_glue_extractor(source: CatSource) -> Tuple[GlueExtractor, Any]:
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
        source: CatSource,
    ) -> Tuple[MysqlMetadataExtractor, Any]:
        where_clause_suffix = """
        WHERE
            c.table_schema NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')
        """

        extractor = MysqlMetadataExtractor()
        scope = extractor.get_scope()
        conn_string_key = f"{scope}.{SQLAlchemyExtractor().get_scope()}.{SQLAlchemyExtractor.CONN_STRING}"

        conf = ConfigFactory.from_dict(
            {
                conn_string_key: source.conn_string,
                f"{scope}.{MysqlMetadataExtractor.CLUSTER_KEY}": source.cluster,
                f"{scope}.{MysqlMetadataExtractor.DATABASE_KEY}": source.name,
                f"{scope}.{MysqlMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}": where_clause_suffix,
            }
        )

        return extractor, conf

    @staticmethod
    def _create_postgres_extractor(source: CatSource) -> Tuple[Extractor, Any]:
        where_clause_suffix = """
            WHERE TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog')
        """
        return DbScanner._create_sqlalchemy_extractor(
            source, where_clause_suffix, PostgresMetadataExtractor
        )

    @staticmethod
    def _create_redshift_extractor(source: CatSource) -> Tuple[Extractor, Any]:
        where_clause_suffix = """
            WHERE TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog')
        """
        return DbScanner._create_sqlalchemy_extractor(
            source, where_clause_suffix, RedshiftMetadataExtractor
        )

    @staticmethod
    def _create_snowflake_extractors(
        source: CatSource,
    ) -> Tuple[SnowflakeMetadataExtractor, Any]:
        extractor = SnowflakeMetadataExtractor()
        scope = extractor.get_scope()
        conn_string_key = f"{scope}.{SQLAlchemyExtractor().get_scope()}.{SQLAlchemyExtractor.CONN_STRING}"

        conf = ConfigFactory.from_dict(
            {
                conn_string_key: source.conn_string,
                f"{scope}.{SnowflakeMetadataExtractor.CLUSTER_KEY}": source.cluster,
                f"{scope}.{SnowflakeMetadataExtractor.DATABASE_KEY}": source.name,
                # f"{scope}.{SnowflakeMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
            }
        )

        return extractor, conf
