import logging
import os
import shutil
import subprocess
from pathlib import Path

import click
import yaml

from dbcat import __version__
from dbcat.catalog.scanners import Scanner
from dbcat.dbcat import render
from dbcat.log_mixin import LogMixin


@click.group()
@click.version_option(__version__)
@click.option("-l", "--log-level", help="Logging Level", default="INFO")
@click.option(
    "--docusaurus-version", help="Specify Docusaurus version", default="2.0.0-alpha.69"
)
@click.option(
    "--working-dir",
    type=click.Path(),
    help="Working Directory to generate documentation",
)
@click.pass_context
def main(ctx, log_level, docusaurus_version, working_dir):
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    ctx.ensure_object(dict)

    ctx.obj["docusaurus_version"] = docusaurus_version

    if working_dir is None:
        working_dir = "~/.dbcat"
    path = Path(working_dir)
    path = path.expanduser().resolve()

    ctx.obj["working_dir"] = path


config_template = """
# Instructions for configuring Tokern DbCat.
# This configuration file is in YAML format.

# The configuration file is a list of databases to scan.
# The following database types are supported:
# Files
# BigQuery
# Copy paste the section of the relevant database and fill in the values.

databases:
  - name: local
    type: file
    path: /dev/null
# - name: bq_db
#   type: bigquery
#   key_path: ers/robert/gcp-credentials.json
#   project_credentials:  # Only one of key_path or project_credentials needed
#   project_id:
"""


@main.command("init", help="Initialize a dbcat working directory")
@click.option(
    "--delete/--no-delete", default=False, help="Delete the working directory"
)
@click.pass_obj
def init(obj, delete):
    working_dir = obj["working_dir"]
    logger = LogMixin()
    logger.logger.debug("Working directory is {}".format(working_dir))

    if working_dir.exists() and delete:
        shutil.rmtree(working_dir)

    if not working_dir.exists():
        os.mkdir(working_dir)
        config = working_dir / "config"
        with open(config, "w") as f:
            f.write(config_template)


@main.command("docs", help="Initialize a docusaurus scaffold")
@click.option("--path", type=click.Path())
@click.pass_obj
def docs(obj, path):

    doc_init = subprocess.run(
        [
            "npx",
            "@docusaurus/init@{}".format(obj["docusaurus_version"]),
            "init",
            path,
            "classic",
        ]
    )

    doc_init.check_returncode()


schema_help_text = """
Scan only schemas matching schema; When this option is not specified, all
non-system schemas in the target database will be dumped. Multiple schemas can
be selected by writing multiple -n switches. Also, the schema parameter is
interpreted as a regular expression, so multiple schemas can also be selected
by writing wildcard characters in the pattern. When using wildcards, be careful
to quote the pattern if needed to prevent the shell from expanding the wildcards;
"""
exclude_schema_help_text = """
Do not scan any schemas matching the schema pattern. The pattern is interpreted
according to the same rules as for -n. -N can be given more than once to exclude
 schemas matching any of several patterns.

When both -n and -N are given, the behavior is to dump just the schemas that
match at least one -n switch but no -N switches. If -N appears without -n, then
schemas matching -N are excluded from what is otherwise a normal dump.")
"""

table_help_text = """
Dump only tables matching table. Multiple tables can be selected by writing
multiple -t switches. Also, the table parameter is interpreted as a regular
expression, so multiple tables can also be selected by writing wildcard
characters in the pattern. When using wildcards, be careful to quote the pattern
 if needed to prevent the shell from expanding the wildcards.

The -n and -N switches have no effect when -t is used, because tables selected
by -t will be dumped regardless of those switches.
"""

exclude_table_help_text = """
Do not dump any tables matching the table pattern. The pattern is interpreted
according to the same rules as for -t. -T can be given more than once to
exclude tables matching any of several patterns.

When both -t and -T are given, the behavior is to dump just the tables that
match at least one -t switch but no -T switches. If -T appears without -t, then
tables matching -T are excluded from what is otherwise a normal dump.
"""


@main.command("generate", help="Generate documents by scanning databases")
@click.option(
    "-d",
    "--db-name",
    help="Name of the database. If not specified, all databases are scanned",
)
@click.option("-n", "--include-schema", multiple=True, help=schema_help_text)
@click.option("-N", "--exclude-schema", multiple=True, help=exclude_schema_help_text)
@click.option("-t", "--include-table", multiple=True, help=table_help_text)
@click.option("-T", "--exclude-table", multiple=True, help=exclude_table_help_text)
@click.argument("path", type=click.Path(exists=True))
@click.pass_obj
def generate(
    obj, db_name, include_schema, exclude_schema, include_table, exclude_table, path
):
    config_file = obj["working_dir"] / "config"
    logger = LogMixin()
    logger.logger.debug("Config file: {}".format(config_file))
    with open(config_file, "r") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    logger.logger.debug(config)
    scanners = Scanner.create(config)
    catalogs = []
    for scanner in scanners:
        catalog = scanner.scan()
        if db_name is None or catalog.name == db_name:
            catalogs.append(catalog)

    for catalog in catalogs:
        catalog.set_include_regex(include_schema)
        catalog.set_exclude_regex(exclude_schema)
        for schema in catalog.schemata:
            schema.set_include_regex(include_table)
            schema.set_exclude_regex(exclude_table)

    render(catalogs, Path(path))
