import logging

import pytest
import yaml
from typer.testing import CliRunner

from dbcat.__main__ import app
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


@pytest.fixture
def config_path(request, load_all_data, open_catalog_connection):
    catalog, conf = open_catalog_connection

    config = yaml.safe_load(conf)
    config_params = []

    for key, value in config["catalog"].items():
        config_params.append("--catalog-{}".format(key))
        config_params.append(value)

    source_params = [
        "--username",
        "piiuser",
        "--password",
        "p11secret",
        "--database",
        "piidb",
        "--uri",
        "127.0.0.1",
    ]

    runner = CliRunner()
    for type in ["add-mysql", "add-postgresql"]:
        result = runner.invoke(
            app,
            config_params
            + ["catalog", type, "--name", "{}_{}".format(type, request.node.name)]
            + source_params,
        )
        print(result.stdout)
        assert result.exit_code == 0

    yield catalog, config_params, request.node.name

    with catalog.managed_session as session:
        logging.debug("Starting clean up of catalog")
        [
            session.delete(db)
            for db in session.query(CatSource)
            .filter(CatSource.name.like("%{}%".format(request.node.name)))
            .all()
        ]


def test_cli_all(config_path):
    catalog, config_params, suffix = config_path

    runner = CliRunner()
    result = runner.invoke(app, config_params + ["catalog", "scan"])
    print(result.stdout)
    assert result.exit_code == 0
    run_asserts(catalog, "add-postgresql_{}".format(suffix))
    run_asserts(catalog, "add-mysql_{}".format(suffix))


@pytest.mark.parametrize("source", ["add-postgresql", "add-mysql"])
def test_cli_connection(config_path, source):
    catalog, config_params, suffix = config_path
    connection_name = "{}_{}".format(source, suffix)

    runner = CliRunner()
    result = runner.invoke(
        app, config_params + ["catalog", "scan", "--source-name", connection_name],
    )
    print(result.stdout)
    assert result.exit_code == 0
    run_asserts(catalog, connection_name)
