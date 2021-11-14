import logging
from enum import Enum
from pathlib import Path
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
        LOGGER.debug(f"Open PG Catalog at {host}")
        return PGCatalog(
            host=host, port=port, user=user, password=password, database=database,
        )
    elif path is not None:
        LOGGER.debug(f"Open Sqlite Catalog at {path}")
        return SqliteCatalog(path=str(path))

    raise AttributeError("None of Path or Postgres connection parameters are provided")


def catalog_connection_yaml(config: str) -> Catalog:
    config_yaml = yaml.safe_load(config)
    LOGGER.debug("Open Catalog from config")
    return catalog_connection(**config_yaml["catalog"])


def open_catalog(
    app_dir: Path,
    path: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
) -> Catalog:
    try:
        return catalog_connection(
            path=path,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
    except AttributeError:
        config_file = app_dir / "catalog.yml"
        if config_file.exists():
            with config_file.open() as f:
                LOGGER.debug(f"Open Catalog from config file {config_file}")
                return catalog_connection_yaml(f.read())
        else:
            LOGGER.debug(f"Open default Sqlite Catalog in {app_dir}/catalog.db")
            return catalog_connection(path=str(app_dir / "catalog.db"))


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
