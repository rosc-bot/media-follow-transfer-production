"""Store Telegram subscriber identifiers as signed 64-bit integers."""

import sqlalchemy as sa
from alembic import op

revision = '0003_watchlist_tg_bigint'
down_revision = '0002_media_pipeline_indexes'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('series_watchlist') as batch_op:
        batch_op.alter_column(
            'subscriber_tg_id',
            existing_type=sa.Integer(),
            type_=sa.BigInteger(),
            existing_nullable=True,
        )


def downgrade() -> None:
    with op.batch_alter_table('series_watchlist') as batch_op:
        batch_op.alter_column(
            'subscriber_tg_id',
            existing_type=sa.BigInteger(),
            type_=sa.Integer(),
            existing_nullable=True,
        )
