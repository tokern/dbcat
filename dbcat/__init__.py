# flake8: noqa

__version__ = "0.8.2"

import logging
from typing import List, Optional

import yaml
from alembic import command

from dbcat.catalog import Catalog
from dbcat.catalog.catalog import PGCatalog, SqliteCatalog
from dbcat.catalog.db import DbScanner
from dbcat.log_mixin import LogMixin
from dbcat.migrations import get_alembic_config

LOGGER = logging.getLogger(__name__)


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


def add_connections(catalog_obj: Catalog, config: str) -> None:
    config_yaml = yaml.safe_load(config)

    with catalog_obj.managed_session:
        for conn in config_yaml["connections"]:
            LOGGER.info("Adding {}".format(conn))
            catalog_obj.add_source(**conn)


def pull(
    catalog_obj: Catalog,
    connection_name: str,
    include_schema_regex_str: Optional[List[str]] = None,
    exclude_schema_regex_str: Optional[List[str]] = None,
    include_table_regex_str: Optional[List[str]] = None,
    exclude_table_regex_str: Optional[List[str]] = None,
) -> None:
    with catalog_obj.managed_session:
        source = catalog_obj.get_source(connection_name)
        LOGGER.debug("Source: {}".format(source))
        scanner = DbScanner(
            catalog_obj,
            source,
            include_schema_regex_str=include_schema_regex_str,
            exclude_schema_regex_str=exclude_schema_regex_str,
            include_table_regex_str=include_table_regex_str,
            exclude_table_regex_str=exclude_table_regex_str,
        )
        LOGGER.info("Scanning {}".format(scanner.name))
        scanner.scan()


def pull_all(catalog_obj: Catalog) -> None:
    with catalog_obj.managed_session:
        for source in catalog_obj.get_sources():
            LOGGER.info("Starting scan on {}".format(source))
            pull(catalog_obj, source.name)
