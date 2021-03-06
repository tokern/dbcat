import yaml

from dbcat.catalog.catalog import Catalog
from dbcat.catalog.db import DbScanner
from dbcat.log_mixin import LogMixin


def catalog_connection(config: str) -> Catalog:
    config_yaml = yaml.safe_load(config)
    return Catalog(**config_yaml["catalog"])


def pull(catalog: Catalog) -> None:
    logger = LogMixin()

    scanners = []
    with catalog:
        for source in catalog.search_sources("%"):
            scanners.append(DbScanner(catalog, source))

    for scanner in scanners:
        logger.logger.debug("Scanning {}".format(scanner.name))
        scanner.scan()
