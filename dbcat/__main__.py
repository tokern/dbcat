import logging
import os
import shutil
from pathlib import Path

import click
import yaml

from dbcat import __version__, pull
from dbcat.catalog.catalog import Catalog
from dbcat.log_mixin import LogMixin


@click.group()
@click.version_option(__version__)
@click.option("-l", "--log-level", help="Logging Level", default="INFO")
@click.option(
    "--config-dir", type=click.Path(), help="Config Directory",
)
@click.pass_context
def main(ctx, log_level, config_dir):
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    ctx.ensure_object(dict)

    if config_dir is None:
        config_dir = "~/.dbcat"
    path = Path(config_dir)
    path = path.expanduser().resolve()

    ctx.obj["config_dir"] = path


config_template = """
# Instructions for configuring Tokern DbCat.
# This configuration file is in YAML format.

# Copy paste the section of the relevant database and fill in the values.

# The configuration file consists of
# - a catalog sink where metadata is stored.
# - a list of database connections to scan.


# The following catalog types are supported:
# - Files
# - Postgres
# - MySQL
# Choose one of them

catalog:
## Postgres/MySQL
  type: "postgres" or "mysql"
  user: db_user
  password: db_password
  host: db_host
  port: db_port

connections:
  - name: pg
    type: postgres
    database: db_database
    username: db_user
    password: db_password
    port: db_port
    uri: db_uri
  - name: mys
    type: mysql
    database: db_database
    username: db_user
    password: db_password
    port: db_port
    uri: db_uri
  - name: bq
    type: bigquery
    key_path: db_key_path
    project_credentials:  db_creds
    project_id: db_project_id
  - name: gl
    type: glue
  - name: sf
    type: snowflake
    database: db_database
    username: db_user
    password: db_password
    account: db_account
    role: db_role
    warehouse: db_warehouse
"""


@main.command("init", help="Initialize a dbcat working directory")
@click.option(
    "--delete/--no-delete", default=False, help="Delete the working directory"
)
@click.pass_obj
def init(obj, delete):
    config_dir = obj["config_dir"]
    logger = LogMixin()
    logger.logger.debug("Config directory is {}".format(config_dir))

    if config_dir.exists() and delete:
        shutil.rmtree(config_dir)

    if not config_dir.exists():
        os.mkdir(config_dir)
        config = config_dir / "config"
        with open(config, "w") as f:
            f.write(config_template)


@main.command("pull", help="Scan and load metadata from databases into the catalog")
@click.option(
    "-c",
    "--connection-name",
    help="Name of the connection. If not specified, all databases are scanned",
)
@click.pass_obj
def pull_cli(
    obj, connection_name,
):
    config_file = obj["config_dir"] / "config"
    logger = LogMixin()
    logger.logger.debug("Config file: {}".format(config_file))
    with open(config_file, "r") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    logger.logger.debug(config)

    catalog = Catalog(**config["catalog"])
    pull(catalog)
