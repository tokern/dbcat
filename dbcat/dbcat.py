import os

from jinja2 import Environment, FileSystemLoader

from dbcat.log_mixin import LogMixin


def render_sidebar(databases, path):
    sidebar_path = path / "sidebars.js"

    logger = LogMixin()

    file_loader = FileSystemLoader("dbcat/templates")
    env = Environment(loader=file_loader)
    sidebar_template = env.get_template("sidebars.js.jinja2")
    output = sidebar_template.render(databases=databases)

    logger.logger.debug("Writing to file: {}".format(sidebar_path))
    with open(sidebar_path, "w") as sidebar_fd:
        sidebar_fd.write(output)


def render(databases, path):
    docs_path = path / "docs"

    file_loader = FileSystemLoader("dbcat/templates")
    env = Environment(loader=file_loader)

    logger = LogMixin()

    for db in databases:
        db_template = env.get_template("database.mdx.jinja2")
        output = db_template.render(database=db)
        db_dir = docs_path / db.name
        logger.logger.debug("Creating directory for database: {}".format(db_dir))
        os.mkdir(db_dir)
        db_mdx = db_dir / "{}.mdx".format(db.name)
        logger.logger.debug("Writing to file: {}".format(db_mdx))
        with open(db_mdx, "w") as db_fd:
            db_fd.write(output)

        for schema in db.schemata:
            schema_template = env.get_template("schema.mdx.jinja2")
            output = schema_template.render(schema=schema)
            schema_dir = db_dir / schema.name
            logger.logger.debug("Creating directory for schema: {}".format(schema_dir))

            os.mkdir(schema_dir)
            schema_mdx = schema_dir / "{}.mdx".format(schema.name)
            logger.logger.debug("Writing to file: {}".format(schema_mdx))
            with open(schema_mdx, "w") as schema_fd:
                schema_fd.write(output)

            for table in schema.tables:
                table_template = env.get_template("table.mdx.jinja2")
                output = table_template.render(table=table)
                table_mdx = schema_dir / "{}.mdx".format(table.name)
                logger.logger.debug("Writing to file: {}".format(table_mdx))

                with open(table_mdx, "w") as table_fd:
                    table_fd.write(output)

    render_sidebar(databases, path)
