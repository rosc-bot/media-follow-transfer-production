"""Single authorization boundary for Telegram OWNER/ADMIN operations."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.admin import AdminAuditLog, TelegramAdmin, TelegramUser

logger = logging.getLogger(__name__)

_SECRET_WORDS = ("token", "secret", "password", "cookie", "refresh", "auth_ref", "credential")


class AdminPermissionError(PermissionError):
    """Raised when a privileged operation is not allowed."""


@dataclass(frozen=True)
class AdminPrincipal:
    telegram_user_id: int
    role: str
    source: str = "database"


def _safe_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _safe_value(item)
            for key, item in value.items()
            if not any(word in str(key).casefold() for word in _SECRET_WORDS)
        }
    if isinstance(value, (list, tuple)):
        return [_safe_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


class AdminService:
    """Primary DB-backed authorization with a narrow ADMIN_TG_ID fail-safe."""

    @staticmethod
    def fallback_owner_id() -> int | None:
        value = get_settings().admin_tg_id
        return int(value) if value is not None else None

    @staticmethod
    async def principal(db: AsyncSession, user_id: int | None) -> AdminPrincipal | None:
        if user_id is None:
            return None
        uid = int(user_id)
        fallback = AdminService.fallback_owner_id()
        try:
            row = await db.scalar(select(TelegramAdmin).where(TelegramAdmin.telegram_user_id == uid))
        except SQLAlchemyError:
            # Only the configured OWNER is allowed through the outage path.
            await db.rollback()
            if fallback is not None and uid == fallback:
                return AdminPrincipal(uid, "OWNER", "ADMIN_TG_ID_FALLBACK")
            return None
        if row is not None:
            # The fallback owner cannot be downgraded or disabled by a bad row.
            if fallback is not None and uid == fallback:
                return AdminPrincipal(uid, "OWNER", "database")
            if not row.enabled:
                return None
            if row.role not in {"OWNER", "ADMIN"}:
                return None
            row.last_seen_at = datetime.now(UTC)
            return AdminPrincipal(uid, row.role, "database")
        if fallback is not None and uid == fallback:
            return AdminPrincipal(uid, "OWNER", "ADMIN_TG_ID_FALLBACK")
        return None

    @staticmethod
    async def is_admin(db: AsyncSession, user_id: int | None) -> bool:
        return await AdminService.principal(db, user_id) is not None

    @staticmethod
    async def is_owner(db: AsyncSession, user_id: int | None) -> bool:
        principal = await AdminService.principal(db, user_id)
        return principal is not None and principal.role == "OWNER"

    @staticmethod
    async def require_admin(db: AsyncSession, user_id: int) -> AdminPrincipal:
        principal = await AdminService.principal(db, user_id)
        if principal is None:
            raise AdminPermissionError("ADMIN_REQUIRED")
        return principal

    @staticmethod
    async def require_owner(db: AsyncSession, user_id: int) -> AdminPrincipal:
        principal = await AdminService.principal(db, user_id)
        if principal is None or principal.role != "OWNER":
            raise AdminPermissionError("OWNER_REQUIRED")
        return principal

    @staticmethod
    def snapshot(row: TelegramAdmin | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "telegram_user_id": int(row.telegram_user_id),
            "username": row.username,
            "display_name": row.display_name,
            "role": row.role,
            "enabled": bool(row.enabled),
            "note": row.note,
            "receive_failure_notifications": bool(row.receive_failure_notifications),
        }

    @staticmethod
    async def record_audit(
        db: AsyncSession,
        *,
        actor_user_id: int,
        actor_role: str,
        action: str,
        target_user_id: int | None = None,
        target_task_id: int | None = None,
        before: Any = None,
        after: Any = None,
    ) -> AdminAuditLog:
        row = AdminAuditLog(
            actor_user_id=int(actor_user_id),
            actor_role=str(actor_role),
            action=str(action),
            target_user_id=int(target_user_id) if target_user_id is not None else None,
            target_task_id=int(target_task_id) if target_task_id is not None else None,
            before_state=_safe_value(before),
            after_state=_safe_value(after),
        )
        db.add(row)
        await db.flush()
        return row

    @staticmethod
    async def record_user(
        db: AsyncSession,
        *,
        telegram_user_id: int,
        username: str | None,
        display_name: str | None,
    ) -> TelegramUser:
        row = await db.scalar(select(TelegramUser).where(TelegramUser.telegram_user_id == int(telegram_user_id)))
        now = datetime.now(UTC)
        if row is None:
            row = TelegramUser(
                telegram_user_id=int(telegram_user_id),
                username=username,
                display_name=display_name,
                last_seen_at=now,
            )
            db.add(row)
        else:
            row.username = username
            row.display_name = display_name
            row.last_seen_at = now
        await db.flush()
        return row

    @staticmethod
    async def recent_users(db: AsyncSession, limit: int = 20) -> list[TelegramUser]:
        return list((await db.scalars(select(TelegramUser).order_by(TelegramUser.last_seen_at.desc()).limit(limit))).all())

    @staticmethod
    async def list_admins(db: AsyncSession) -> list[TelegramAdmin]:
        try:
            rows = list((await db.scalars(
                select(TelegramAdmin).order_by(TelegramAdmin.role.asc(), TelegramAdmin.enabled.desc(), TelegramAdmin.telegram_user_id.asc())
            )).all())
        except SQLAlchemyError:
            await db.rollback()
            rows = []
        fallback = AdminService.fallback_owner_id()
        if fallback is not None and not any(int(row.telegram_user_id) == fallback for row in rows):
            rows.insert(0, TelegramAdmin(
                telegram_user_id=fallback,
                username=None,
                display_name="ADMIN_TG_ID fallback OWNER",
                role="OWNER",
                enabled=True,
            ))
        return rows

    @staticmethod
    async def add_admin(
        db: AsyncSession,
        *,
        actor_user_id: int,
        target_user_id: int,
        username: str | None = None,
        display_name: str | None = None,
        note: str | None = None,
    ) -> TelegramAdmin:
        actor = await AdminService.require_owner(db, actor_user_id)
        target_id = int(target_user_id)
        fallback = AdminService.fallback_owner_id()
        if fallback is not None and target_id == fallback:
            raise AdminPermissionError("OWNER_CANNOT_BE_MODIFIED")
        row = await db.scalar(select(TelegramAdmin).where(TelegramAdmin.telegram_user_id == target_id))
        before = AdminService.snapshot(row)
        if row is None:
            row = TelegramAdmin(
                telegram_user_id=target_id,
                username=username,
                display_name=display_name,
                role="ADMIN",
                enabled=True,
                note=note,
                created_by=int(actor_user_id),
                last_seen_at=datetime.now(UTC),
            )
            db.add(row)
        else:
            if row.role == "OWNER":
                raise AdminPermissionError("OWNER_CANNOT_BE_MODIFIED")
            # Re-adding the same ID is idempotent; do not silently re-enable a disabled admin.
            if username is not None:
                row.username = username
            if display_name is not None:
                row.display_name = display_name
            if note is not None:
                row.note = note
            row.last_seen_at = datetime.now(UTC)
        await db.flush()
        await AdminService.record_audit(
            db,
            actor_user_id=actor_user_id,
            actor_role=actor.role,
            action="ADD_ADMIN",
            target_user_id=target_id,
            before=before,
            after=AdminService.snapshot(row),
        )
        return row

    @staticmethod
    async def remove_admin(db: AsyncSession, *, actor_user_id: int, target_user_id: int) -> bool:
        actor = await AdminService.require_owner(db, actor_user_id)
        target_id = int(target_user_id)
        row = await db.scalar(select(TelegramAdmin).where(TelegramAdmin.telegram_user_id == target_id))
        if row is None:
            return False
        if row.role == "OWNER" or target_id == AdminService.fallback_owner_id():
            raise AdminPermissionError("OWNER_CANNOT_BE_MODIFIED")
        before = AdminService.snapshot(row)
        await db.delete(row)
        await db.flush()
        await AdminService.record_audit(
            db,
            actor_user_id=actor_user_id,
            actor_role=actor.role,
            action="REMOVE_ADMIN",
            target_user_id=target_id,
            before=before,
            after=None,
        )
        return True

    @staticmethod
    async def set_enabled(db: AsyncSession, *, actor_user_id: int, target_user_id: int, enabled: bool) -> TelegramAdmin:
        actor = await AdminService.require_owner(db, actor_user_id)
        target_id = int(target_user_id)
        row = await db.scalar(select(TelegramAdmin).where(TelegramAdmin.telegram_user_id == target_id))
        if row is None:
            raise AdminPermissionError("ADMIN_NOT_FOUND")
        if row.role == "OWNER" or target_id == AdminService.fallback_owner_id():
            raise AdminPermissionError("OWNER_CANNOT_BE_MODIFIED")
        before = AdminService.snapshot(row)
        row.enabled = bool(enabled)
        await db.flush()
        await AdminService.record_audit(
            db,
            actor_user_id=actor_user_id,
            actor_role=actor.role,
            action="ENABLE_ADMIN" if enabled else "DISABLE_ADMIN",
            target_user_id=target_id,
            before=before,
            after=AdminService.snapshot(row),
        )
        return row

    @staticmethod
    async def require_target_admin(db: AsyncSession, *, actor_user_id: int, target_user_id: int) -> TelegramAdmin:
        await AdminService.require_owner(db, actor_user_id)
        row = await db.scalar(select(TelegramAdmin).where(TelegramAdmin.telegram_user_id == int(target_user_id)))
        if row is None or row.role != "ADMIN":
            raise AdminPermissionError("ADMIN_NOT_FOUND")
        return row
