import logging
from enum import Enum
from typing import List, Optional

import yaml
from alembic import command

from dbcat.catalog import Catalog
from dbcat.catalog.catalog import PGCatalog, SqliteCatalog
from dbcat.catalog.db import DbScanner
from dbcat.migrations import get_alembic_config

LOGGER = logging.getLogger(__name__)


class OutputFormat(str, Enum):
    tabular = "tabular"
    json = "json"


def catalog_connection(
    path: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
) -> Catalog:
    if (
        host is not None
        and user is not None
        and password is not None
        and database is not None
    ):
        return PGCatalog(
            host=host, port=port, user=user, password=password, database=database,
        )
    elif path is not None:
        return SqliteCatalog(path=str(path))

    raise AttributeError("None of Path or Postgres connection parameters are provided")


def catalog_connection_yaml(config: str) -> Catalog:
    config_yaml = yaml.safe_load(config)
    return catalog_connection(**config_yaml["catalog"])


def init_db(catalog_obj: Catalog) -> None:
    """
    Initialize database
    """
    LOGGER.info("Initializing the database")

    config = get_alembic_config(catalog_obj.engine)
    LOGGER.debug(config)
    command.upgrade(config, "heads")


def scan_sources(
    catalog: Catalog,
    source_names: Optional[List[str]] = None,
    include_schema_regex: Optional[List[str]] = None,
    exclude_schema_regex: Optional[List[str]] = None,
    include_table_regex: Optional[List[str]] = None,
    exclude_table_regex: Optional[List[str]] = None,
):
    with catalog.managed_session:
        if source_names is not None and len(source_names) > 0:
            sources = [catalog.get_source(source_name) for source_name in source_names]
        else:
            sources = catalog.get_sources()

        for source in sources:
            scanner = DbScanner(
                catalog,
                source,
                include_schema_regex_str=include_schema_regex,
                exclude_schema_regex_str=exclude_schema_regex,
                include_table_regex_str=include_table_regex,
                exclude_table_regex_str=exclude_table_regex,
            )
            LOGGER.info("Scanning {}".format(scanner.name))
            scanner.scan()
