"""Telegram roles, administrator ledger, and fixed channel separation.

Revision ID: 0013_telegram_roles_and_admins
Revises: 0012_resource_candidates
"""

import os

import sqlalchemy as sa
from alembic import op

revision = "0013_telegram_roles_and_admins"
down_revision = "0012_resource_candidates"
branch_labels = None
depends_on = None


def _upsert_channel(bind, *, channel_id: str, channel_name: str, role: str) -> None:
    exists = bind.execute(
        sa.text("SELECT 1 FROM channel_settings WHERE channel_id = :channel_id"),
        {"channel_id": channel_id},
    ).first()
    if exists:
        bind.execute(
            sa.text(
                "UPDATE channel_settings SET channel_name=:channel_name, enabled=true, role=:role, "
                "transfer_mode='OFF', accept_forward=false, updated_at=now() WHERE channel_id=:channel_id"
            ),
            {"channel_id": channel_id, "channel_name": channel_name, "role": role},
        )
    else:
        bind.execute(
            sa.text(
                "INSERT INTO channel_settings "
                "(channel_id, channel_name, enabled, role, transfer_mode, accept_forward, created_at, updated_at) "
                "VALUES (:channel_id, :channel_name, true, :role, 'OFF', false, now(), now())"
            ),
            {"channel_id": channel_id, "channel_name": channel_name, "role": role},
        )


def upgrade() -> None:
    op.create_table(
        "telegram_admins",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False),
        sa.Column("username", sa.String(length=255)),
        sa.Column("display_name", sa.String(length=512)),
        sa.Column("role", sa.String(length=16), nullable=False, server_default="ADMIN"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("note", sa.Text()),
        sa.Column("created_by", sa.BigInteger()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("last_seen_at", sa.DateTime(timezone=True)),
        sa.Column("receive_failure_notifications", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.UniqueConstraint("telegram_user_id", name="uq_telegram_admin_user_id"),
        sa.CheckConstraint("role IN ('OWNER', 'ADMIN')", name="ck_telegram_admin_role"),
    )
    op.create_index("ix_telegram_admin_enabled_role", "telegram_admins", ["enabled", "role"])
    op.create_index(
        "uq_telegram_single_owner",
        "telegram_admins",
        ["role"],
        unique=True,
        postgresql_where=sa.text("role = 'OWNER'"),
        sqlite_where=sa.text("role = 'OWNER'"),
    )

    op.create_table(
        "telegram_users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False),
        sa.Column("username", sa.String(length=255)),
        sa.Column("display_name", sa.String(length=512)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("telegram_user_id", name="uq_telegram_user_id"),
    )

    op.create_table(
        "admin_audit_log",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("actor_user_id", sa.BigInteger(), nullable=False),
        sa.Column("actor_role", sa.String(length=16), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("target_user_id", sa.BigInteger()),
        sa.Column("target_task_id", sa.Integer()),
        sa.Column("before", sa.JSON()),
        sa.Column("after", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_admin_audit_created_at", "admin_audit_log", ["created_at"])
    op.create_index("ix_admin_audit_actor_action", "admin_audit_log", ["actor_user_id", "action"])

    bind = op.get_bind()
    raw_owner = os.getenv("ADMIN_TG_ID", "8586984520").strip()
    owner_id = int(raw_owner or "8586984520")
    existing_owner = bind.execute(
        sa.text("SELECT telegram_user_id FROM telegram_admins WHERE role='OWNER' AND telegram_user_id <> :owner_id LIMIT 1"),
        {"owner_id": owner_id},
    ).first()
    if existing_owner is not None:
        raise RuntimeError("telegram_admins already contains a different OWNER; refusing migration")
    owner_exists = bind.execute(
        sa.text("SELECT 1 FROM telegram_admins WHERE telegram_user_id=:owner_id"),
        {"owner_id": owner_id},
    ).first()
    if owner_exists:
        bind.execute(
            sa.text("UPDATE telegram_admins SET role='OWNER', enabled=true, updated_at=now() WHERE telegram_user_id=:owner_id"),
            {"owner_id": owner_id},
        )
    else:
        bind.execute(
            sa.text(
                "INSERT INTO telegram_admins "
                "(telegram_user_id, role, enabled, created_by, created_at, updated_at, last_seen_at, receive_failure_notifications) "
                "VALUES (:owner_id, 'OWNER', true, NULL, now(), now(), now(), true)"
            ),
            {"owner_id": owner_id},
        )

    # The success channel is notification-only; it must no longer feed Scout.
    _upsert_channel(bind, channel_id="-1004387965244", channel_name="@guangyazhauncun", role="SUCCESS_NOTIFICATION")
    # Future public publishing is explicitly isolated from ingest and Scout.
    _upsert_channel(bind, channel_id="-1004332079561", channel_name="@guangyaziyuanfenxiang", role="PUBLISH_ONLY")


def downgrade() -> None:
    op.drop_index("ix_admin_audit_actor_action", table_name="admin_audit_log")
    op.drop_index("ix_admin_audit_created_at", table_name="admin_audit_log")
    op.drop_table("admin_audit_log")
    op.drop_table("telegram_users")
    op.drop_index("uq_telegram_single_owner", table_name="telegram_admins")
    op.drop_index("ix_telegram_admin_enabled_role", table_name="telegram_admins")
    op.drop_table("telegram_admins")
