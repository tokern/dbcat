# flake8: noqa

__version__ = "0.5.0"

import yaml

from dbcat.catalog import Catalog
from dbcat.dbcat import pull  # type: ignore


def catalog_connection(config: str) -> Catalog:
    config_yaml = yaml.safe_load(config)
    return Catalog(**config_yaml["catalog"])
