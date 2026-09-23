"""Split legacy global pause into safe follow and transfer pause gates.

Revision ID: 0010_split_worker_pauses
Revises: 0009_remote_series_layout

The migration only inserts missing control rows with a fail-closed value.  It
never changes global_pause, queue rows, watchlist rows, collected episodes, or
cloud/Telegram state.  Downgrade intentionally leaves safety rows intact.
"""

import sqlalchemy as sa
from alembic import op

revision = "0010_split_worker_pauses"
down_revision = "0009_remote_series_layout"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    for key in ("follow_paused", "transfer_paused"):
        bind.execute(
            sa.text(
                "INSERT INTO bot_settings (key, val) VALUES (:key, '1') "
                "ON CONFLICT (key) DO NOTHING"
            ),
            {"key": key},
        )


def downgrade() -> None:
    # Preserve operator safety state and historical setting timestamps.
    pass
