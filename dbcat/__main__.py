import logging
import logging.config
from pathlib import Path
from typing import Optional

import typer

import dbcat.settings
from dbcat import __version__
from dbcat.cli import app as catalog_app
from dbcat.settings import OutputFormat

app = typer.Typer()


class TyperLoggerHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        fg = None
        bg = None
        if record.levelno == logging.DEBUG:
            fg = typer.colors.YELLOW
        elif record.levelno == logging.INFO:
            fg = typer.colors.BRIGHT_BLUE
        elif record.levelno == logging.WARNING:
            fg = typer.colors.BRIGHT_MAGENTA
        elif record.levelno == logging.CRITICAL:
            fg = typer.colors.BRIGHT_RED
        elif record.levelno == logging.ERROR:
            fg = typer.colors.BRIGHT_WHITE
            bg = typer.colors.RED
        typer.secho(self.format(record), bg=bg, fg=fg)


def log_config(log_level: str):
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "{asctime}:{levelname}:{name} {message}",
                "style": "{",
            },
        },
        "handlers": {
            "console": {"class": "logging.StreamHandler", "formatter": "simple"},
            "typer": {
                "class": "dbcat.__main__.TyperLoggerHandler",
                "formatter": "simple",
            },
        },
        "root": {"handlers": ["typer"], "level": log_level},
        "dbcat": {"handlers": ["typer"], "level": log_level},
        "sqlachemy": {"handlers": ["typer"], "level": "DEBUG"},
        "alembic": {"handlers": ["typer"], "level": "DEBUG"},
        "databuilder": {"handlers": ["typer"], "level": "INFO"},
    }


def version_callback(value: bool):
    if value:
        print("{}".format(__version__))
        typer.Exit()


@app.callback()
def cli(
    log_level: str = typer.Option("WARNING", help="Logging Level"),
    output_format: OutputFormat = typer.Option(
        OutputFormat.tabular, case_sensitive=False
    ),
    catalog_path: Path = typer.Option(
        None, help="Path to store catalog state. Use if NOT using a database"
    ),
    catalog_host: str = typer.Option(
        None, help="hostname of Postgres database. Use if catalog is a database."
    ),
    catalog_port: int = typer.Option(
        None, help="port of Postgres database. Use if catalog is a database."
    ),
    catalog_user: str = typer.Option(
        None, help="user of Postgres database. Use if catalog is a database."
    ),
    catalog_password: str = typer.Option(
        None, help="password of Postgres database. Use if catalog is a database."
    ),
    catalog_database: str = typer.Option(
        None, help="database of Postgres database. Use if catalog is a database."
    ),
    catalog_secret: str = typer.Option(
        "TOKERN_CATALOG_SECRET",
        help="Secret to encrypt sensitive data like passwords in the catalog.",
    ),
    version: Optional[bool] = typer.Option(
        None, "--version", callback=version_callback
    ),
):
    logging.config.dictConfig(log_config(log_level=log_level.upper()))

    app_dir = typer.get_app_dir("tokern")
    app_dir_path = Path(app_dir)
    app_dir_path.mkdir(parents=True, exist_ok=True)

    dbcat.settings.CATALOG_PATH = catalog_path
    dbcat.settings.CATALOG_USER = catalog_user
    dbcat.settings.CATALOG_PASSWORD = catalog_password
    dbcat.settings.CATALOG_HOST = catalog_host
    dbcat.settings.CATALOG_PORT = catalog_port
    dbcat.settings.CATALOG_DB = catalog_database
    dbcat.settings.CATALOG_SECRET = catalog_secret
    dbcat.settings.APP_DIR = app_dir_path
    dbcat.settings.OUTPUT_FORMAT = output_format


app.add_typer(catalog_app, name="catalog")
