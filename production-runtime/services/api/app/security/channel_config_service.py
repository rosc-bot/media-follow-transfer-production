"""Owner-only runtime channel configuration with role isolation."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.constants import CHANNEL_ROLE_PUBLISH_ONLY, CHANNEL_ROLE_SUCCESS_NOTIFICATION
from app.follow.bot_settings_service import BotSettingsService
from app.models.channel import ChannelSetting
from app.security.admin_service import AdminService
from app.transfer.notifier import TransferNotifier


class ChannelConfigService:
    SUCCESS_KEY = "transfer_success_chat"
    PUBLISH_KEY = "resource_publish_chat"

    @staticmethod
    async def targets(db: AsyncSession) -> dict[str, str]:
        settings = get_settings()
        return {
            "transfer_success_chat": await BotSettingsService.get(db, ChannelConfigService.SUCCESS_KEY, settings.transfer_success_chat) or settings.transfer_success_chat,
            "resource_publish_chat": await BotSettingsService.get(db, ChannelConfigService.PUBLISH_KEY, settings.resource_publish_chat) or settings.resource_publish_chat,
        }

    @staticmethod
    async def _set_fixed_target(
        db: AsyncSession,
        *,
        actor_user_id: int,
        key: str,
        chat_id: str,
        role: str,
        action: str,
    ) -> str:
        actor = await AdminService.require_owner(db, actor_user_id)
        normalized = TransferNotifier._normalize_chat_identifier(chat_id)
        if normalized is None:
            raise ValueError("TARGET_INVALID")
        target = str(normalized)
        previous = await BotSettingsService.get(db, key, None)
        await BotSettingsService.set(db, key, target)
        row = await db.scalar(select(ChannelSetting).where(ChannelSetting.channel_id == target))
        if row is None:
            row = ChannelSetting(
                channel_id=target,
                channel_name=target,
                enabled=True,
                role=role,
                transfer_mode="OFF",
                accept_forward=False,
            )
            db.add(row)
        else:
            row.role = role
            row.enabled = True
            row.transfer_mode = "OFF"
            row.accept_forward = False
        await db.flush()
        await AdminService.record_audit(
            db,
            actor_user_id=actor_user_id,
            actor_role=actor.role,
            action=action,
            before={"chat_id": previous, "role": None},
            after={"chat_id": target, "role": role},
        )
        return target

    @staticmethod
    async def set_success_chat(db: AsyncSession, *, actor_user_id: int, chat_id: str) -> str:
        return await ChannelConfigService._set_fixed_target(
            db,
            actor_user_id=actor_user_id,
            key=ChannelConfigService.SUCCESS_KEY,
            chat_id=chat_id,
            role=CHANNEL_ROLE_SUCCESS_NOTIFICATION,
            action="SET_SUCCESS_NOTIFICATION_CHANNEL",
        )

    @staticmethod
    async def set_publish_chat(db: AsyncSession, *, actor_user_id: int, chat_id: str) -> str:
        return await ChannelConfigService._set_fixed_target(
            db,
            actor_user_id=actor_user_id,
            key=ChannelConfigService.PUBLISH_KEY,
            chat_id=chat_id,
            role=CHANNEL_ROLE_PUBLISH_ONLY,
            action="SET_PUBLISH_CHANNEL",
        )

    @staticmethod
    async def set_channel_role(db: AsyncSession, *, actor_user_id: int, channel_id: str, role: str) -> ChannelSetting:
        actor = await AdminService.require_owner(db, actor_user_id)
        if role not in {"RESOURCE", "MANUAL_INGEST", CHANNEL_ROLE_PUBLISH_ONLY, CHANNEL_ROLE_SUCCESS_NOTIFICATION}:
            raise ValueError("CHANNEL_ROLE_INVALID")
        row = await db.scalar(select(ChannelSetting).where(ChannelSetting.channel_id == str(channel_id)))
        if row is None:
            row = ChannelSetting(channel_id=str(channel_id), role=role, enabled=True, transfer_mode="OFF", accept_forward=False)
            db.add(row)
        else:
            before = {"channel_id": row.channel_id, "role": row.role, "enabled": row.enabled, "transfer_mode": row.transfer_mode}
            row.role = role
            if role in {CHANNEL_ROLE_PUBLISH_ONLY, CHANNEL_ROLE_SUCCESS_NOTIFICATION}:
                row.transfer_mode = "OFF"
                row.accept_forward = False
            after = {"channel_id": row.channel_id, "role": row.role, "enabled": row.enabled, "transfer_mode": row.transfer_mode}
            await db.flush()
            await AdminService.record_audit(db, actor_user_id=actor_user_id, actor_role=actor.role, action="SET_CHANNEL_ROLE", after=after, before=before)
            return row
        await db.flush()
        await AdminService.record_audit(
            db,
            actor_user_id=actor_user_id,
            actor_role=actor.role,
            action="SET_CHANNEL_ROLE",
            after={"channel_id": row.channel_id, "role": row.role, "enabled": row.enabled, "transfer_mode": row.transfer_mode},
        )
        return row
