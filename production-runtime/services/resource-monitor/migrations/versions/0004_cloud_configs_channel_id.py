"""Add channel_id column to cloud_configs for provider-level channel routing."""

import sqlalchemy as sa
from alembic import op

revision = '0004_cloud_configs_channel_id'
down_revision = '0003_watchlist_tg_bigint'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('cloud_configs') as batch_op:
        batch_op.add_column(sa.Column('channel_id', sa.String(64), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('cloud_configs') as batch_op:
        batch_op.drop_column('channel_id')
