import logging

import pytest
from click.testing import CliRunner

from dbcat.__main__ import main
from dbcat.catalog import CatSource


def run_asserts(catalog, connection_name):
    with catalog.managed_session:
        pg_source = catalog.get_source(connection_name)
        assert pg_source is not None

        pg_schemata = pg_source.schemata
        assert len(pg_schemata) == 1

        pg_tables = pg_schemata[0].tables
        assert len(pg_tables) == 3

        pg_columns = []
        for table in pg_tables:
            pg_columns = pg_columns + table.columns
        assert len(pg_columns) == 6


connection_config = """
{catalog_conf}
connections:
  - name: pg_{test_name}
    source_type: postgresql
    database: piidb
    username: piiuser
    password: p11secret
    uri: 127.0.0.1
  - name: mysql_{test_name}
    source_type: mysql
    database: piidb
    username: piiuser
    password: p11secret
    uri: 127.0.0.1
"""


@pytest.fixture
def config_path(tmp_path_factory, request, load_all_data, open_catalog_connection):
    catalog, conf = open_catalog_connection

    config_dir = tmp_path_factory.mktemp(request.node.name)
    config_file = config_dir / "config"
    config_file.write_text(
        connection_config.format(test_name=request.node.name, catalog_conf=conf)
    )
    yield catalog, config_dir, request.node.name

    with catalog.managed_session as session:
        logging.debug("Starting clean up of catalog")
        [
            session.delete(db)
            for db in session.query(CatSource)
            .filter(CatSource.name.like("%{}%".format(request.node.name)))
            .all()
        ]


def test_cli_all(config_path):
    catalog, config_path, suffix = config_path

    runner = CliRunner()
    result = runner.invoke(main, ["--config-dir", config_path, "pull"])
    print(result.stdout)
    assert result.exit_code == 0
    run_asserts(catalog, "pg_{}".format(suffix))
    run_asserts(catalog, "mysql_{}".format(suffix))


@pytest.mark.parametrize("source", ["pg", "mysql"])
def test_cli_connection(config_path, source):
    catalog, config_path, suffix = config_path
    connection_name = "{}_{}".format(source, suffix)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--log-level",
            "DEBUG",
            "--config-dir",
            config_path,
            "pull",
            "--connection-names",
            connection_name,
        ],
    )
    print(result.stdout)
    assert result.exit_code == 0
    run_asserts(catalog, connection_name)
