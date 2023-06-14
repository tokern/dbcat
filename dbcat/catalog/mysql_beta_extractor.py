# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import namedtuple
from itertools import groupby
from typing import (
    Any, Dict, Iterator, Union,
)

from pyhocon import ConfigFactory, ConfigTree

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata

TableKey = namedtuple('TableKey', ['schema', 'table_name'])

LOGGER = logging.getLogger(__name__)


class MysqlbetaMetadataExtractor(Extractor):
    """
    Extracts mysql table and column metadata from underlying meta store database using SQLAlchemyExtractor
    """
    # SELECT statement from mysql information_schema to extract table and column metadata
    SQL_STATEMENT = """
        SELECT
            c.column_name AS col_name,
            c.column_comment AS col_description,
            c.data_type AS col_type,
            c.ordinal_position AS col_sort_order,
            c.table_catalog AS cluster,
            c.table_schema AS "schema",
            c.table_name AS name,
            t.table_comment AS description,
            case when lower(t.table_type) = "view" then "true" else "false" end AS is_view
            FROM
            INFORMATION_SCHEMA.TABLES t
            left outer JOIN INFORMATION_SCHEMA.COLUMNS AS c
                on c.TABLE_SCHEMA = t.TABLE_SCHEMA
                and c.TABLE_NAME = t.TABLE_NAME
            {where_clause_suffix}
            ORDER BY ccluster, "schema", name, col_sort_order  ;
    """

    # CONFIG KEYS
    WHERE_CLAUSE_SUFFIX_KEY = 'where_clause_suffix'
    CLUSTER_KEY = 'cluster_key'
    USE_CATALOG_AS_CLUSTER_NAME = 'use_catalog_as_cluster_name'
    DATABASE_KEY = 'database_key'

    # Default values
    DEFAULT_CLUSTER_NAME = 'master'

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {WHERE_CLAUSE_SUFFIX_KEY: ' ', CLUSTER_KEY: DEFAULT_CLUSTER_NAME, USE_CATALOG_AS_CLUSTER_NAME: True}
    )

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(MysqlbetaMetadataExtractor.DEFAULT_CONFIG)
        self._cluster = conf.get_string(MysqlbetaMetadataExtractor.CLUSTER_KEY)

        if conf.get_bool(MysqlbetaMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME):
            cluster_source = "c.table_catalog"
        else:
            cluster_source = f"'{self._cluster}'"

        self._database = conf.get_string(MysqlbetaMetadataExtractor.DATABASE_KEY, default='mysql')

        self.sql_stmt = MysqlbetaMetadataExtractor.SQL_STATEMENT.format(
            where_clause_suffix=conf.get_string(MysqlbetaMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY),
            cluster_source=cluster_source
        )

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, self._alchemy_extractor.get_scope()) \
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt}))

        self.sql_stmt = sql_alch_conf.get_string(SQLAlchemyExtractor.EXTRACT_SQL)

        LOGGER.info('SQL for mysql metadata: %s', self.sql_stmt)

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter: Union[None, Iterator] = None

    def extract(self) -> Union[TableMetadata, None]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.mysql_metadata'

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        """
        Using itertools.groupby and raw level iterator, it groups to table and yields TableMetadata
        :return:
        """
        for key, group in groupby(self._get_raw_extract_iter(), self._get_table_key):
            columns = []

            for row in group:
                last_row = row
                columns.append(ColumnMetadata(row['col_name'], row['col_description'],
                                              row['col_type'], row['col_sort_order']))

            yield TableMetadata(self._database, last_row['cluster'],
                                last_row['schema'],
                                last_row['name'],
                                last_row['description'],
                                columns,
                                is_view=last_row['is_view'])

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        """
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        """
        row = self._alchemy_extractor.extract()
        while row:
            yield row
            row = self._alchemy_extractor.extract()

    def _get_table_key(self, row: Dict[str, Any]) -> Union[TableKey, None]:
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            return TableKey(schema=row['schema'], table_name=row['name'])

        return None