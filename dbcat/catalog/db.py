import logging
import re
from contextlib import closing
from typing import Any, Generator, List, Optional, Pattern, Tuple, Type

from databuilder import Scoped
from databuilder.extractor.athena_metadata_extractor import AthenaMetadataExtractor
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
from dbcat.catalog.sqlite_extractor import SqliteMetadataExtractor

LOGGER = logging.getLogger(__name__)


class DbScanner:
    def __init__(
        self,
        catalog: Catalog,
        source: CatSource,
        include_schema_regex_str: Optional[List[str]] = None,
        exclude_schema_regex_str: Optional[List[str]] = None,
        include_table_regex_str: Optional[List[str]] = None,
        exclude_table_regex_str: Optional[List[str]] = None,
    ):
        self._name = source.name
        self._extractor: Extractor
        self._conf: ConfigTree
        self._catalog = catalog
        self._source = source
        if source.source_type == "bigquery":
            self._extractor, self._conf = DbScanner._create_big_query_extractor(source)
        elif source.source_type == "glue":
            self._extractor, self._conf = DbScanner._create_glue_extractor(source)
        elif source.source_type == "mysql":
            self._extractor, self._conf = DbScanner._create_mysql_extractor(source)
        elif source.source_type == "postgresql":
            self._extractor, self._conf = DbScanner._create_postgres_extractor(source)
        elif source.source_type == "redshift":
            self._extractor, self._conf = DbScanner._create_redshift_extractor(source)
        elif source.source_type == "snowflake":
            self._extractor, self._conf = DbScanner._create_snowflake_extractor(source)
        elif source.source_type == "sqlite":
            self._extractor, self._conf = DbScanner._create_sqlite_extractor(source)
        elif source.source_type == "athena":
            self._extractor, self._conf = DbScanner._create_athena_extractor(source)
        else:
            raise ValueError("{} is not supported".format(source.source_type))

        self.include_schema_regex: Optional[List[Pattern]] = None
        self.exclude_schema_regex: Optional[List[Pattern]] = None
        self.include_table_regex: Optional[List[Pattern]] = None
        self.exclude_table_regex: Optional[List[Pattern]] = None

        if include_schema_regex_str is not None and len(include_schema_regex_str) > 0:
            self.include_schema_regex = [
                re.compile(exp, re.IGNORECASE) for exp in include_schema_regex_str
            ]

        if exclude_schema_regex_str is not None and len(exclude_schema_regex_str) > 0:
            self.exclude_schema_regex = [
                re.compile(exp, re.IGNORECASE) for exp in exclude_schema_regex_str
            ]

        if include_table_regex_str is not None and len(include_table_regex_str) > 0:
            self.include_table_regex = [
                re.compile(exp, re.IGNORECASE) for exp in include_table_regex_str
            ]

        if exclude_table_regex_str is not None and len(exclude_table_regex_str) > 0:
            self.exclude_table_regex = [
                re.compile(exp, re.IGNORECASE) for exp in exclude_table_regex_str
            ]

    @property
    def name(self):
        return self._name

    @staticmethod
    def _test_regex(
        name: str,
        include_regex: Optional[List[Pattern]] = None,
        exclude_regex: Optional[List[Pattern]] = None,
    ) -> bool:

        passed = False
        if include_regex is not None and len(include_regex) > 0:
            for regex in include_regex:
                passed |= regex.search(name) is not None
        else:
            passed = True

        if exclude_regex is not None and len(exclude_regex) > 0:
            for regex in exclude_regex:
                passed &= regex.search(name) is None

        return passed

    def _filter_rows(
        self, extractor: Extractor
    ) -> Generator[TableMetadata, None, None]:
        record = extractor.extract()
        while record:
            if DbScanner._test_regex(
                record.schema, self.include_schema_regex, self.exclude_schema_regex
            ) and DbScanner._test_regex(
                record.name, self.include_table_regex, self.exclude_table_regex
            ):
                yield record
            record = extractor.extract()
        return None

    def scan(self):
        schema_count = 0
        table_count = 0
        column_count = 0
        with closing(self._extractor) as extractor:
            extractor.init(Scoped.get_scoped_conf(self._conf, extractor.get_scope()))

            record: TableMetadata = next(self._filter_rows(extractor))
            current_schema = self._catalog.add_schema(
                schema_name=record.schema, source=self._source
            )
            schema_count += 1
            LOGGER.info(f"Start extraction of schema {record.schema}")
            while record:
                LOGGER.debug(record)
                if record.schema != current_schema.name:
                    LOGGER.debug(f"Total tables extracted: {table_count}")
                    current_schema = self._catalog.add_schema(
                        schema_name=record.schema, source=self._source
                    )
                    LOGGER.debug(f"Start extraction of schema {record.schema}")
                    schema_count += 1

                table = self._catalog.add_table(
                    table_name=record.name, schema=current_schema
                )
                table_count += 1
                index = 0
                for c in record.columns:
                    self._catalog.add_column(
                        column_name=c.name,
                        data_type=c.type,
                        sort_order=index,
                        table=table,
                    )
                    index += 1
                    column_count += 1
                try:
                    record = next(self._filter_rows(extractor))
                except StopIteration:
                    record = None

        LOGGER.info(
            "Scanned {} schemata, {} tables, {} columns".format(
                schema_count, table_count, column_count
            )
        )

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
                f"{scope}.{extractorClass.DATABASE_KEY}": source.database,
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
                f"{scope}.{MysqlMetadataExtractor.DATABASE_KEY}": source.database,
                f"{scope}.{MysqlMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}": where_clause_suffix,
            }
        )

        return extractor, conf

    @staticmethod
    def _create_postgres_extractor(source: CatSource) -> Tuple[Extractor, Any]:
        where_clause_suffix = """
            st.schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        """
        return DbScanner._create_sqlalchemy_extractor(
            source, where_clause_suffix, PostgresMetadataExtractor
        )

    @staticmethod
    def _create_redshift_extractor(source: CatSource) -> Tuple[Extractor, Any]:
        where_clause_suffix = """
            WHERE SCHEMA NOT IN ('information_schema', 'pg_catalog')
        """
        return DbScanner._create_sqlalchemy_extractor(
            source, where_clause_suffix, RedshiftMetadataExtractor
        )

    @staticmethod
    def _create_snowflake_extractor(
        source: CatSource,
    ) -> Tuple[SnowflakeMetadataExtractor, Any]:
        extractor = SnowflakeMetadataExtractor()
        scope = extractor.get_scope()
        conn_string_key = f"{scope}.{SQLAlchemyExtractor().get_scope()}.{SQLAlchemyExtractor.CONN_STRING}"

        conf = ConfigFactory.from_dict(
            {
                conn_string_key: source.conn_string,
                f"{scope}.{SnowflakeMetadataExtractor.CLUSTER_KEY}": source.cluster,
                f"{scope}.{SnowflakeMetadataExtractor.DATABASE_KEY}": source.database,
                f"{scope}.{SnowflakeMetadataExtractor.SNOWFLAKE_DATABASE_KEY}": source.database,
                # f"{scope}.{SnowflakeMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
            }
        )

        return extractor, conf

    @staticmethod
    def _create_athena_extractor(
        source: CatSource,
    ) -> Tuple[AthenaMetadataExtractor, Any]:
        extractor = AthenaMetadataExtractor()
        scope = extractor.get_scope()
        conn_string_key = f"{scope}.{SQLAlchemyExtractor().get_scope()}.{SQLAlchemyExtractor.CONN_STRING}"

        conf = ConfigFactory.from_dict(
            {
                conn_string_key: source.conn_string,
                f"{scope}.{AthenaMetadataExtractor.CATALOG_KEY}": source.cluster,
            }
        )

        return extractor, conf

    @staticmethod
    def _create_sqlite_extractor(
        source: CatSource,
    ) -> Tuple[SqliteMetadataExtractor, Any]:
        extractor = SqliteMetadataExtractor()
        scope = extractor.get_scope()
        conn_string_key = f"{scope}.{SQLAlchemyExtractor().get_scope()}.{SQLAlchemyExtractor.CONN_STRING}"
        conf = ConfigFactory.from_dict(
            {
                conn_string_key: source.conn_string,
                f"{scope}.{SqliteMetadataExtractor.CLUSTER_KEY}": source.name,
            }
        )

        return extractor, conf
