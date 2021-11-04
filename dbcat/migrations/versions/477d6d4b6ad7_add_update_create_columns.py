"""add update create columns

Revision ID: 477d6d4b6ad7
Revises: d2c711b84996
Create Date: 2021-11-04 10:52:09.569288

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "477d6d4b6ad7"
down_revision = "d2c711b84996"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("sources", sa.Column("created_at", sa.TIMESTAMP))
    op.add_column("sources", sa.Column("updated_at", sa.TIMESTAMP))

    op.add_column("jobs", sa.Column("created_at", sa.TIMESTAMP))
    op.add_column("jobs", sa.Column("updated_at", sa.TIMESTAMP))

    op.add_column("schemata", sa.Column("created_at", sa.TIMESTAMP))
    op.add_column("schemata", sa.Column("updated_at", sa.TIMESTAMP))

    op.add_column("default_schema", sa.Column("created_at", sa.TIMESTAMP))
    op.add_column("default_schema", sa.Column("updated_at", sa.TIMESTAMP))

    op.add_column("job_executions", sa.Column("created_at", sa.TIMESTAMP))
    op.add_column("job_executions", sa.Column("updated_at", sa.TIMESTAMP))

    op.add_column("tables", sa.Column("created_at", sa.TIMESTAMP))
    op.add_column("tables", sa.Column("updated_at", sa.TIMESTAMP))

    op.add_column("columns", sa.Column("created_at", sa.TIMESTAMP))
    op.add_column("columns", sa.Column("updated_at", sa.TIMESTAMP))

    op.add_column("column_lineage", sa.Column("created_at", sa.TIMESTAMP))
    op.add_column("column_lineage", sa.Column("updated_at", sa.TIMESTAMP))


def downgrade():
    op.drop_column("sources", "created_at")
    op.drop_column("sources", "updated_at")

    op.drop_column("jobs", "created_at")
    op.drop_column("jobs", "updated_at")

    op.drop_column("schemata", "created_at")
    op.drop_column("schemata", "updated_at")

    op.drop_column("default_schema", "created_at")
    op.drop_column("default_schema", "updated_at")

    op.drop_column("job_executions", "created_at")
    op.drop_column("job_executions", "updated_at")

    op.drop_column("tables", "created_at")
    op.drop_column("tables", "updated_at")

    op.drop_column("columns", "created_at")
    op.drop_column("columns", "updated_at")

    op.drop_column("column_lineage", "created_at")
    op.drop_column("column_lineage", "updated_at")
