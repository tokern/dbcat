import logging
from enum import Enum
from pathlib import Path
from typing import List, Optional

import yaml
from alembic import command
from sqlalchemy.orm.exc import NoResultFound

import dbcat.settings
from dbcat.catalog import Catalog, CatSource
from dbcat.catalog.catalog import PGCatalog, SqliteCatalog
from dbcat.catalog.db import DbScanner
from dbcat.generators import NoMatchesError
from dbcat.migrations import get_alembic_config

LOGGER = logging.getLogger(__name__)


class OutputFormat(str, Enum):
    tabular = "tabular"
    json = "json"


def init_db(catalog_obj: Catalog) -> None:
    """
    Initialize database
    """

    config = get_alembic_config(catalog_obj.engine)
    command.upgrade(config, "heads")
    LOGGER.info("Initialized the database")


def catalog_connection(
    secret: str,
    path: Optional[Path] = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
) -> Catalog:
    dbcat.settings.CATALOG_SECRET = secret

    if (
        host is not None
        and user is not None
        and password is not None
        and database is not None
    ):
        LOGGER.info(f"Open PG Catalog at {host}")
        return PGCatalog(
            host=host, port=port, user=user, password=password, database=database,
        )
    elif path is not None:
        LOGGER.info(f"Open Sqlite Catalog at {path}")
        return SqliteCatalog(path=str(path))

    raise AttributeError("None of Path or Postgres connection parameters are provided")


def catalog_connection_yaml(config: str) -> Catalog:
    config_yaml = yaml.safe_load(config)
    LOGGER.debug("Open Catalog from config")
    if "path" in config_yaml and config_yaml["path"] is not None:
        config_yaml["path"] = Path(config_yaml["path"])
    return catalog_connection(**config_yaml["catalog"])


def open_catalog(
    app_dir: Path,
    secret: str,
    path: Optional[Path] = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
) -> Catalog:
    try:
        catalog = catalog_connection(
            secret=secret,
            path=path,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
    except AttributeError:
        LOGGER.info("No catalog options given as parameters.")
        config_file = app_dir / "catalog.yml"
        if config_file.exists():
            with config_file.open() as f:
                LOGGER.debug("Open Catalog from config file %s", config_file)
                catalog = catalog_connection_yaml(f.read())
        else:
            LOGGER.debug("Open default Sqlite Catalog in %s/catalog.db", app_dir)
            catalog = catalog_connection(
                path=app_dir / "catalog.db",
                secret=dbcat.settings.DEFAULT_CATALOG_SECRET,
            )

    init_db(catalog_obj=catalog)
    return catalog


def scan_sources(
    catalog: Catalog,
    source_names: Optional[List[str]] = None,
    include_schema_regex: Optional[List[str]] = None,
    exclude_schema_regex: Optional[List[str]] = None,
    include_table_regex: Optional[List[str]] = None,
    exclude_table_regex: Optional[List[str]] = None,
):
    with catalog.managed_session:
        if source_names is not None and len(source_names) > 0:
            sources: List[CatSource] = []
            for source_name in source_names:
                try:
                    sources.append(catalog.get_source(source_name))
                except NoResultFound:
                    LOGGER.error("Source '%s' not found", source_name)
        else:
            sources = catalog.get_sources()

        LOGGER.info("%d sources will be scanned", len(sources))
        for source in sources:
            scanner = DbScanner(
                catalog,
                source,
                include_schema_regex_str=include_schema_regex,
                exclude_schema_regex_str=exclude_schema_regex,
                include_table_regex_str=include_table_regex,
                exclude_table_regex_str=exclude_table_regex,
            )
            LOGGER.info("Scanning {}".format(scanner.name))
            try:
                scanner.scan()
            except StopIteration:
                raise NoMatchesError


def add_sqlite_source(
    catalog: Catalog, name: str, path: Path,
):
    with catalog.managed_session:
        catalog.add_source(name=name, uri=str(path), source_type="sqlite")


def add_postgresql_source(
    catalog: Catalog,
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            username=username,
            password=password,
            database=database,
            uri=uri,
            port=port,
            source_type="postgresql",
        )


def add_mysql_source(
    catalog: Catalog,
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            username=username,
            password=password,
            database=database,
            uri=uri,
            port=port,
            source_type="mysql",
        )


def add_redshift_source(
    catalog: Catalog,
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            username=username,
            password=password,
            database=database,
            uri=uri,
            port=port,
            source_type="redshift",
        )


def add_snowflake_source(
    catalog: Catalog,
    name: str,
    account: str,
    username: str,
    password: str,
    database: str,
    warehouse: str,
    role: str,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            username=username,
            password=password,
            database=database,
            account=account,
            warehouse=warehouse,
            role=role,
            source_type="snowflake",
        )


def add_athena_source(
    catalog: Catalog,
    name: str,
    region_name: str,
    s3_staging_dir: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    mfa: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            s3_staging_dir=s3_staging_dir,
            mfa = mfa,
            aws_session_token=aws_session_token,
            source_type="athena",
        )

def add_bigquery_source(
    catalog: Catalog,
    name: str,
    username:str,
    project_id: str,
    key_path: str,
) -> CatSource:
    with catalog.commit_context:
            return catalog.add_source(
                name=name,
                username=username,
                project_id = project_id,
                key_path=key_path,
                source_type="bigquery",
            )
