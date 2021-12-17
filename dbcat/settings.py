from enum import Enum
from pathlib import Path

CATALOG_PATH: Path

CATALOG_USER: str

CATALOG_PASSWORD: str

CATALOG_HOST: str

CATALOG_PORT: int

CATALOG_DB: str

CATALOG_SECRET: str

APP_DIR: Path


class OutputFormat(str, Enum):
    tabular = "tabular"
    json = "json"


OUTPUT_FORMAT: OutputFormat = None  # type: ignore

DEFAULT_CATALOG_SECRET: str = "TOKERN_DEFAULT_CATALOG_SECRET"
