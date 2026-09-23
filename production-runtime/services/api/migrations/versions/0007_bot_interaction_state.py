"""Create Bot interaction state tables.

Revision ID: 0007_bot_interaction_state
Revises: 0006_fix_ingest_job_unique
Create Date: 2026-09-21
"""

import sqlalchemy as sa
from alembic import op

revision = "0007_bot_interaction_state"
down_revision = "0006_fix_ingest_job_unique"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bot_settings",
        sa.Column("key", sa.String(length=255), primary_key=True),
        sa.Column("val", sa.String(length=4096), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_table(
        "ignored_missing",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("episode", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("title", "season", "episode", name="uq_ignored_missing_title_season_ep"),
    )
    op.create_table(
        "auto_ingest_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("episodes", sa.JSON(), nullable=True),
        sa.Column("share_url", sa.String(length=2048), nullable=True),
        sa.Column("provider", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("title", "season", "share_url", name="uq_auto_ingest_title_season_url"),
    )
    op.create_table(
        "failed_scout_pushes",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("episodes", sa.JSON(), nullable=True),
        sa.Column("share_url", sa.String(length=2048), nullable=False),
        sa.Column("provider", sa.String(length=255), nullable=True),
        sa.Column("text_context", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="FAILED"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("failed_scout_pushes")
    op.drop_table("auto_ingest_history")
    op.drop_table("ignored_missing")
    op.drop_table("bot_settings")
