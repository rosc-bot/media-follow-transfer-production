"""Allow replacement of terminal-invalid Resource candidates by lifecycle.

Revision ID: 0011_resource_lifecycle_dedup
Revises: 0010_split_worker_pauses

No Resource data is deleted or rewritten. The old global unique constraint is
replaced by a partial unique index that remains strict for live candidates.
"""

import sqlalchemy as sa
from alembic import op

revision = "0011_resource_lifecycle_dedup"
down_revision = "0010_split_worker_pauses"
branch_labels = None
depends_on = None

_NON_BLOCKING = "'FAILED', 'REJECTED', 'INVALID', 'EXPIRED'"
_LIVE_PREDICATE = f"status NOT IN ({_NON_BLOCKING})"


def upgrade() -> None:
    op.drop_constraint("uq_resource_identity_key", "resources", type_="unique")
    op.create_index(
        "uq_resource_live_identity_key",
        "resources",
        ["identity_key"],
        unique=True,
        postgresql_where=sa.text(_LIVE_PREDICATE),
        sqlite_where=sa.text(_LIVE_PREDICATE),
    )


def downgrade() -> None:
    bind = op.get_bind()
    duplicate = bind.execute(
        sa.text(
            "SELECT identity_key FROM resources "
            "GROUP BY identity_key HAVING count(*) > 1 LIMIT 1"
        )
    ).scalar_one_or_none()
    if duplicate is not None:
        raise RuntimeError(
            "Cannot restore global Resource identity uniqueness without deleting "
            f"history; duplicate identity exists: {duplicate}"
        )
    op.drop_index("uq_resource_live_identity_key", table_name="resources")
    op.create_unique_constraint("uq_resource_identity_key", "resources", ["identity_key"])
