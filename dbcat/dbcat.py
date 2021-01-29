from typing import List

from dbcat.catalog.metadata import Connection
from dbcat.catalog.orm import Catalog as CatConnection
from dbcat.log_mixin import LogMixin
from dbcat.scanners.db import DbScanner


def pull(catalog: CatConnection, connections: List[Connection]) -> None:
    scanners = [DbScanner(connection) for connection in connections]

    logger = LogMixin()
    for scanner in scanners:
        logger.logger.debug("Scanning {}".format(scanner.name))
        database = scanner.scan()
        catalog.save_catalog(database)
