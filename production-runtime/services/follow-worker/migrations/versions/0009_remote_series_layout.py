"""Persist remote series roots so completed shows can be promoted safely.

Revision ID: 0009_remote_series_layout
Revises: 0008_destination_roots
"""

import sqlalchemy as sa
from alembic import op

revision = '0009_remote_series_layout'
down_revision = '0008_destination_roots'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('series_watchlist') as batch_op:
        batch_op.add_column(sa.Column('remote_series_folder_id', sa.String(length=256), nullable=True))
        batch_op.add_column(sa.Column('remote_destination_kind', sa.String(length=32), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('series_watchlist') as batch_op:
        batch_op.drop_column('remote_destination_kind')
        batch_op.drop_column('remote_series_folder_id')