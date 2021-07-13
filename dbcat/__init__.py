# flake8: noqa

__version__ = "0.6.0"

import logging

import yaml
from alembic import command

from dbcat.catalog import Catalog
from dbcat.catalog.db import DbScanner
from dbcat.log_mixin import LogMixin
from dbcat.migrations import get_alembic_config

LOGGER = logging.getLogger(__name__)


def catalog_connection(config: str) -> Catalog:
    config_yaml = yaml.safe_load(config)
    return Catalog(**config_yaml["catalog"])


def init_db(catalog_obj: Catalog) -> None:
    """
    Initialize database
    """
    LOGGER.info("Initializing the database")

    config = get_alembic_config(catalog_obj.engine)
    command.upgrade(config, "heads")


def add_connections(catalog_obj: Catalog, config: str) -> None:
    config_yaml = yaml.safe_load(config)

    for conn in config_yaml["connections"]:
        LOGGER.info("Adding {}".format(conn))
        catalog_obj.add_source(**conn)


def pull(catalog_obj: Catalog, connection_name: str) -> None:
    source = catalog_obj.get_source(connection_name)
    LOGGER.debug("Source: {}".format(source))
    scanner = DbScanner(catalog_obj, source)
    LOGGER.info("Scanning {}".format(scanner.name))
    scanner.scan()


def pull_all(catalog_obj: Catalog) -> None:
    for source in catalog_obj.search_sources("%"):
        LOGGER.info("Starting scan on {}".format(source))
        pull(catalog_obj, source.name)
