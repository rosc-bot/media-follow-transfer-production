"""Telegram administrator, recent-user and immutable audit models."""

from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class TelegramAdmin(Base):
    __tablename__ = "telegram_admins"
    __table_args__ = (
        UniqueConstraint("telegram_user_id", name="uq_telegram_admin_user_id"),
        CheckConstraint("role IN ('OWNER', 'ADMIN')", name="ck_telegram_admin_role"),
        Index("ix_telegram_admin_enabled_role", "enabled", "role"),
        Index(
            "uq_telegram_single_owner",
            "role",
            unique=True,
            postgresql_where=text("role = 'OWNER'"),
            sqlite_where=text("role = 'OWNER'"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    username: Mapped[str | None] = mapped_column(String(255))
    display_name: Mapped[str | None] = mapped_column(String(512))
    role: Mapped[str] = mapped_column(String(16), nullable=False, default="ADMIN")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    note: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    receive_failure_notifications: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")


class TelegramUser(Base):
    """Users who interacted with the Bot, used for OWNER-selected admin adds."""

    __tablename__ = "telegram_users"
    __table_args__ = (UniqueConstraint("telegram_user_id", name="uq_telegram_user_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    username: Mapped[str | None] = mapped_column(String(255))
    display_name: Mapped[str | None] = mapped_column(String(512))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class AdminAuditLog(Base):
    """Non-secret audit ledger for privileged Bot operations."""

    __tablename__ = "admin_audit_log"
    __table_args__ = (
        Index("ix_admin_audit_created_at", "created_at"),
        Index("ix_admin_audit_actor_action", "actor_user_id", "action"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    actor_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    actor_role: Mapped[str] = mapped_column(String(16), nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    target_user_id: Mapped[int | None] = mapped_column(BigInteger)
    target_task_id: Mapped[int | None] = mapped_column(Integer)
    before_state: Mapped[dict | None] = mapped_column("before", JSON)
    after_state: Mapped[dict | None] = mapped_column("after", JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
