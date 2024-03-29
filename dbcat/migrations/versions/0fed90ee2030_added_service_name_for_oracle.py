"""added service_name for Oracle

Revision ID: 0fed90ee2030
Revises: 377a3427ceb4
Create Date: 2023-09-20 12:53:25.954797

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0fed90ee2030'
down_revision = '377a3427ceb4'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('sources', sa.Column('service_name', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('sources', 'service_name')
    # ### end Alembic commands ###
