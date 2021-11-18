from contextlib import closing
from pathlib import Path
from typing import List, Optional

import typer

from dbcat.api import init_db, open_catalog, scan_sources
from dbcat.app_state import app_state
from dbcat.generators import NoMatchesError

schema_help_text = """
Scan only schemas matching schema; When this option is not specified, all
non-system schemas in the target database will be dumped. Multiple schemas can
be selected by writing multiple --include switches. Also, the schema parameter is
interpreted as a regular expression, so multiple schemas can also be selected
by writing wildcard characters in the pattern. When using wildcards, be careful
to quote the pattern if needed to prevent the shell from expanding the wildcards;
"""
exclude_schema_help_text = """
Do not scan any schemas matching the schema pattern. The pattern is interpreted
according to the same rules as for --include. --exclude can be given more than once to exclude
 schemas matching any of several patterns.

When both --include and ---exclude are given, the behavior is to dump just the schemas that
match at least one --include switch but no --exclude switches.
If --exclude appears without --include, then schemas matching --exclude are excluded from what
is otherwise a normal scan.")
"""
table_help_text = """
Scan only tables matching table. Multiple tables can be selected by writing
multiple switches. Also, the table parameter is interpreted as a regular
expression, so multiple tables can also be selected by writing wildcard
characters in the pattern. When using wildcards, be careful to quote the pattern
 if needed to prevent the shell from expanding the wildcards.
"""
exclude_table_help_text = """
Do not scan any tables matching the table pattern. The pattern is interpreted
according to the same rules as for --include. --exclude can be given more than once to
exclude tables matching any of several patterns.

When both switches are given, the behavior is to dump just the tables that
match at least one --include switch but no --exclude switches. If --exclude appears without
--include, then tables matching --exclude are excluded from what is otherwise a normal scan.
"""


app = typer.Typer()


@app.command()
def scan(
    source_name: Optional[List[str]] = typer.Option(
        None, help="List of names of database and data warehouses"
    ),
    include_schema: Optional[List[str]] = typer.Option(None, help=schema_help_text),
    exclude_schema: Optional[List[str]] = typer.Option(
        None, help=exclude_schema_help_text
    ),
    include_table: Optional[List[str]] = typer.Option(None, help=table_help_text),
    exclude_table: Optional[List[str]] = typer.Option(
        None, help=exclude_table_help_text
    ),
):
    catalog = open_catalog(**app_state["catalog_connection"])
    with closing(catalog):
        init_db(catalog)
        try:
            scan_sources(
                catalog=catalog,
                source_names=source_name,
                include_schema_regex=include_schema,
                exclude_schema_regex=exclude_schema,
                include_table_regex=include_table,
                exclude_table_regex=exclude_table,
            )
        except NoMatchesError:
            typer.echo(
                "No schema or tables scanned. Ensure include/exclude patterns are correct "
                "and database has tables"
            )


@app.command()
def add_sqlite(
    name: str = typer.Option(..., help="A memorable name for the database"),
    path: Path = typer.Option(..., help="File path to SQLite database"),
):
    catalog = open_catalog(**app_state["catalog_connection"])
    with closing(catalog):
        init_db(catalog)

        with catalog.managed_session:
            catalog.add_source(name=name, source_type="sqlite", path=path)
        typer.echo("Registered SQLite database {}".format(name))


@app.command()
def add_postgresql(
    name: str = typer.Option(..., help="A memorable name for the database"),
    username: str = typer.Option(..., help="Username or role to connect database"),
    password: str = typer.Option(..., help="Password of username or role"),
    database: str = typer.Option(..., help="Database name"),
    uri: str = typer.Option(..., help="Hostname or URI of the database"),
    port: Optional[int] = typer.Option(None, help="Port number of the database"),
):
    catalog = open_catalog(**app_state["catalog_connection"])
    with closing(catalog):
        init_db(catalog)

        with catalog.managed_session:
            catalog.add_source(
                name=name,
                source_type="postgresql",
                username=username,
                password=password,
                database=database,
                uri=uri,
                port=port,
            )
        typer.echo("Registered Postgres database {}".format(name))


@app.command()
def add_mysql(
    name: str = typer.Option(..., help="A memorable name for the database"),
    username: str = typer.Option(..., help="Username or role to connect database"),
    password: str = typer.Option(..., help="Password of username or role"),
    database: str = typer.Option(..., help="Database name"),
    uri: str = typer.Option(..., help="Hostname or URI of the database"),
    port: Optional[int] = typer.Option(None, help="Port number of the database"),
):
    catalog = open_catalog(**app_state["catalog_connection"])
    with closing(catalog):
        init_db(catalog)

        with catalog.managed_session:
            catalog.add_source(
                name=name,
                source_type="mysql",
                username=username,
                password=password,
                database=database,
                uri=uri,
                port=port,
            )
        typer.echo("Registered Postgres database {}".format(name))


@app.command()
def add_redshift(
    name: str = typer.Option(..., help="A memorable name for the database"),
    username: str = typer.Option(..., help="Username or role to connect database"),
    password: str = typer.Option(..., help="Password of username or role"),
    database: str = typer.Option(..., help="Database name"),
    uri: str = typer.Option(..., help="Hostname or URI of the database"),
    port: Optional[int] = typer.Option(None, help="Port number of the database"),
):
    catalog = open_catalog(**app_state["catalog_connection"])
    with closing(catalog):
        init_db(catalog)

        with catalog.managed_session:
            catalog.add_source(
                name=name,
                source_type="redshift",
                username=username,
                password=password,
                database=database,
                uri=uri,
                port=port,
            )
        typer.echo("Registered MySQL database {}".format(name))


@app.command()
def add_snowflake(
    name: str = typer.Option(..., help="A memorable name for the database"),
    username: str = typer.Option(..., help="Username or role to connect database"),
    password: str = typer.Option(..., help="Password of username or role"),
    database: str = typer.Option(..., help="Database name"),
    account: str = typer.Option(..., help="Snowflake Account Name"),
    warehouse: str = typer.Option(..., help="Snowflake Warehouse Name"),
    role: str = typer.Option(..., help="Snowflake Role Name"),
):
    catalog = open_catalog(**app_state["catalog_connection"])
    with closing(catalog):
        init_db(catalog)

        with catalog.managed_session:
            catalog.add_source(
                name=name,
                source_type="snowflake",
                username=username,
                password=password,
                database=database,
                account=account,
                warehouse=warehouse,
                role=role,
            )
        typer.echo("Registered Snowflake database {}".format(name))


@app.command()
def add_athena(
    name: str = typer.Option(..., help="A memorable name for the database"),
    aws_access_key_id: str = typer.Option(..., help="AWS Access Key"),
    aws_secret_access_key: str = typer.Option(..., help="AWS Secret Key"),
    region_name: str = typer.Option(..., help="AWS Region Name"),
    s3_staging_dir: str = typer.Option(..., help="S3 Staging Dir"),
):
    catalog = open_catalog(**app_state["catalog_connection"])
    with closing(catalog):
        init_db(catalog)

        with catalog.managed_session:
            catalog.add_source(
                name=name,
                source_type="snowflake",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
                s3_staging_dir=s3_staging_dir,
            )
        typer.echo("Registered AWS Athena {}".format(name))
