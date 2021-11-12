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
    catalog_path: str = None,
    catalog_host: str = None,
    catalog_port: int = None,
    catalog_user: str = None,
    catalog_password: str = None,
    catalog_database: str = None,
) -> Catalog:
    if (
        catalog_host is not None
        and catalog_port is not None
        and catalog_user is not None
        and catalog_password is not None
        and catalog_database is not None
    ):
        return PGCatalog(
            host=catalog_host,
            port=str(catalog_port),
            user=catalog_user,
            password=catalog_password,
            database=catalog_database,
        )
    elif catalog_path is not None:
        return SqliteCatalog(path=str(catalog_path))

    raise AttributeError("None of Path or Postgres connection parameters are provided")


def catalog_connection_yaml(config: str) -> Catalog:
    config_yaml = yaml.safe_load(config)
    if "path" in config_yaml["catalog"]:
        return SqliteCatalog(**config_yaml["catalog"])
    else:
        return PGCatalog(**config_yaml["catalog"])


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
