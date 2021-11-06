"""add athena columns

Revision ID: 3509a9f07432
Revises: 7d2638f2ab5f
Create Date: 2021-11-06 23:18:49.163209

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3509a9f07432"
down_revision = "7d2638f2ab5f"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("sources", sa.Column("aws_access_key_id", sa.String))
    op.add_column("sources", sa.Column("aws_secret_access_key", sa.String))
    op.add_column("sources", sa.Column("region_name", sa.String))
    op.add_column("sources", sa.Column("s3_staging_dir", sa.String))


def downgrade():
    op.drop_column("sources", "aws_access_key_id")
    op.drop_column("sources", "aws_secret_access_key")
    op.drop_column("sources", "region_name")
    op.drop_column("sources", "s3_staging_dir")
