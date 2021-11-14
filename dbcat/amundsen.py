import logging
from contextlib import closing
from pathlib import Path
from typing import Generator, Union

import typer
from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata
from pyhocon import ConfigTree

from dbcat.api import open_catalog
from dbcat.generators import table_generator

LOGGER = logging.getLogger(__name__)


class CatalogExtractor(Extractor):
    # Config keys
    CATALOG_CONFIG = "catalog_config"
    SOURCE_NAMES = "source_names"
    INCLUDE_SCHEMA_REGEX = "include_schema_regex"
    EXCLUDE_SCHEMA_REGEX = "exclude_schema_regex"
    INCLUDE_TABLE_REGEX = "include_table_regex"
    EXCLUDE_TABLE_REGEX = "exclude_table_regex"

    """
    An Extractor that extracts records via CSV.
    """

    def init(self, conf: ConfigTree) -> None:
        """
        :param conf:
        """
        self.conf = conf
        self.catalog_config = Scoped.get_scoped_conf(
            conf, CatalogExtractor.CATALOG_CONFIG
        )
        self.source_names = conf.get_list(CatalogExtractor.SOURCE_NAMES, default=[])
        self.include_schema_regex = conf.get_list(
            CatalogExtractor.INCLUDE_SCHEMA_REGEX, default=[]
        )
        self.exclude_schema_regex = conf.get_list(
            CatalogExtractor.EXCLUDE_SCHEMA_REGEX, default=[]
        )
        self.include_table_regex = conf.get_list(
            CatalogExtractor.INCLUDE_TABLE_REGEX, default=[]
        )
        self.exclude_table_regex = conf.get_list(
            CatalogExtractor.EXCLUDE_TABLE_REGEX, default=[]
        )

        self.iter = self._load_catalog()

    def _load_catalog(self) -> Generator[TableMetadata, None, None]:
        """
        Create an iterator.
        """
        LOGGER.debug(self.catalog_config.as_plain_ordered_dict())

        catalog = open_catalog(
            app_dir=Path(typer.get_app_dir("tokern")),
            **self.catalog_config.as_plain_ordered_dict()
        )
        with closing(catalog) as catalog:
            with catalog.managed_session:
                if self.source_names is not None and len(self.source_names) > 0:
                    sources = [
                        catalog.get_source(source_name)
                        for source_name in self.source_names
                    ]
                else:
                    sources = catalog.get_sources()

                for source in sources:
                    for schema, table in table_generator(
                        catalog=catalog,
                        source=source,
                        include_schema_regex_str=self.include_schema_regex,
                        exclude_schema_regex_str=self.exclude_schema_regex,
                        include_table_regex_str=self.include_table_regex,
                        exclude_table_regex_str=self.exclude_table_regex,
                    ):
                        columns = []
                        for column in catalog.get_columns_for_table(table):
                            badges = []
                            if column.pii_type is not None:
                                badges.append("pii")
                                badges.append(column.pii_type.name)
                            columns.append(
                                ColumnMetadata(
                                    name=column.name,
                                    description="",
                                    col_type=column.data_type,
                                    sort_order=column.sort_order,
                                    badges=badges,
                                )
                            )
                        yield TableMetadata(
                            database=source.database,
                            cluster=source.name,
                            schema=schema.name,
                            name=table.name,
                            columns=columns,
                            description="",
                        )

    def extract(self) -> Union[TableMetadata, None]:
        try:
            return next(self.iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return "tokern.catalog"
