"""add plugin column

Revision ID: 377a3427ceb4
Revises: 3509a9f07432
Create Date: 2021-11-14 22:02:20.455343

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "377a3427ceb4"
down_revision = "3509a9f07432"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("columns", sa.Column("pii_plugin", sa.String))


def downgrade():
    op.drop_column("columns", "pii_plugin")
