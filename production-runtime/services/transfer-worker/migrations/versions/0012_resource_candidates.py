"""Phase 2C: persistent resource candidate ledger.

Revision ID: 0012_resource_candidates
Revises: 0011_resource_lifecycle_dedup

Adds the resource_candidates table recording every discovered share for a
tmdb/season/episode (usable or not), enabling automatic switch-resource to
exclude permanently-dead candidates while keeping temporary failures retryable.

Read-only phase: no existing data is touched; this migration only creates the
new table (and its dedup constraints).
"""

import sqlalchemy as sa
from alembic import op

revision = "0012_resource_candidates"
down_revision = "0011_resource_lifecycle_dedup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "resource_candidates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tmdb_id", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(512), nullable=False),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("episode_key", sa.String(64), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("share_url", sa.Text(), nullable=False),
        sa.Column("share_hash", sa.String(128), nullable=False),
        sa.Column("source_type", sa.String(32), nullable=True),
        sa.Column("source_channel_id", sa.String(128), nullable=True),
        sa.Column("source_message_id", sa.Integer(), nullable=True),
        sa.Column("resource_id", sa.Integer(), nullable=True),
        sa.Column("queue_task_id", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("failure_category", sa.String(64), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "tmdb_id", "season", "episode_key", "share_hash",
            name="uq_candidate_episode_hash",
        ),
        sa.Index("ix_candidate_episode_status", "tmdb_id", "season", "episode_key", "status"),
        sa.Index("ix_candidate_hash", "share_hash"),
    )


def downgrade() -> None:
    op.drop_table("resource_candidates")
