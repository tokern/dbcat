# flake8: noqa

__version__ = "0.5.5"

import logging

import yaml

from dbcat.catalog import Catalog
from dbcat.catalog.db import DbScanner
from dbcat.log_mixin import LogMixin


def catalog_connection(config: str) -> Catalog:
    config_yaml = yaml.safe_load(config)
    return Catalog(**config_yaml["catalog"])


def add_connections(catalog: Catalog, config: str) -> None:
    config_yaml = yaml.safe_load(config)

    for conn in config_yaml["connections"]:
        logging.debug("Adding {}".format(conn))
        catalog.add_source(**conn)


def pull(catalog_obj: Catalog, connection_name: str) -> None:
    source = catalog_obj.get_source(connection_name)
    logging.debug("Source: {}".format(source))
    scanner = DbScanner(catalog_obj, source)
    logging.debug("Scanning {}".format(scanner.name))
    scanner.scan()


def pull_all(catalog_obj: Catalog) -> None:
    for source in catalog_obj.search_sources("%"):
        logging.debug("Starting scan on {}".format(source))
        pull(catalog_obj, source.name)
