import yaml

from dbcat.catalog import Catalog
from dbcat.scanners.json import File


def test_file_scanner():
    scanner = File("test", "test/catalog.json")
    catalog = scanner.scan()
    assert catalog.name == "test"
    assert len(catalog.schemata) == 1

    default_schema = catalog.schemata[0]
    assert default_schema.name == "default"
    assert len(default_schema.tables) == 8


postgres_conf = """
catalog:
  type: postgres
  user: db_user
  password: db_password
  host: db_host
  port: db_port
"""


def test_catalog_config():
    config = yaml.safe_load(postgres_conf)
    conn: Catalog = Catalog(**config["catalog"])
    assert conn.type == "postgres"
    assert conn.user == "db_user"
    assert conn.host == "db_host"
    assert conn.port == "db_port"
