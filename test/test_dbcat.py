from contextlib import closing
from typing import List

import yaml

from dbcat import pull
from dbcat.catalog.metadata import Connection
from dbcat.catalog.orm import Catalog as CatConnection

conf = """
catalog:
  type: postgres
  user: catalog_user
  password: catal0g_passw0rd
  host: 127.0.0.1
  port: 5432
  database: tokern
connections:
  - name: pg
    type: postgres
    database: piidb
    username: piiuser
    password: p11secret
    port: 5432
    uri: 127.0.0.1
  - name: mys
    type: mysql
    database: piidb
    username: piiuser
    password: p11secret
    port: 3306
    uri: 127.0.0.1
"""


def test_api(load_all_data, catalog_connection):
    config = yaml.safe_load(conf)
    catalog = CatConnection(**config["catalog"])
    connections: List[Connection] = [
        Connection(**conn) for conn in config["connections"]
    ]

    with closing(catalog) as catalog:
        pull(catalog, connections)
