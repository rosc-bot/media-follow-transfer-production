"""Separate completed and ongoing cloud roots; persist TMDB series lifecycle."""

import sqlalchemy as sa
from alembic import op

revision = '0008_destination_roots'
down_revision = '0007_bot_interaction_state'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('cloud_configs') as batch_op:
        batch_op.add_column(sa.Column('ongoing_target_folder_id', sa.String(length=256), nullable=True))
    with op.batch_alter_table('series_watchlist') as batch_op:
        batch_op.add_column(sa.Column('tmdb_series_status', sa.String(length=64), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('series_watchlist') as batch_op:
        batch_op.drop_column('tmdb_series_status')
    with op.batch_alter_table('cloud_configs') as batch_op:
        batch_op.drop_column('ongoing_target_folder_id')
