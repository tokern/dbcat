import os

from alembic.config import Config

import dbcat


def get_alembic_config(engine) -> Config:
    """
    Get alembic config
    """
    library_dir = os.path.dirname(os.path.abspath(dbcat.__file__))
    migration_path = os.path.join(library_dir, "migrations")
    config = Config(os.path.join(library_dir, "alembic.ini"))
    config.set_main_option("script_location", migration_path.replace("%", "%%"))
    config.attributes["connection"] = engine

    return config
