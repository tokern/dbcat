from typing import List, Tuple

import yaml

from dbcat.catalog.metadata import Connection
from dbcat.catalog.orm import Catalog, CatColumn, ColumnLineage
from dbcat.log_mixin import LogMixin
from dbcat.scanners.db import DbScanner


def catalog_connection(config: str) -> Catalog:
    config_yaml = yaml.safe_load(config)
    return Catalog(**config_yaml["catalog"])


def add_edge(
    catalog: Catalog, source: CatColumn, target: CatColumn
) -> Tuple[ColumnLineage, bool]:
    return catalog.add_column_lineage(
        source_name=source, target_name=target, payload={}
    )


def pull(catalog: Catalog, connections: List[Connection]) -> None:
    scanners = [DbScanner(connection) for connection in connections]

    logger = LogMixin()
    for scanner in scanners:
        logger.logger.debug("Scanning {}".format(scanner.name))
        database = scanner.scan()
        catalog.save_catalog(database)
