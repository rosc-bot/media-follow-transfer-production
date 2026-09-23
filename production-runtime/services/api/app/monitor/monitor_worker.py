import logging
from collections.abc import Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.constants import CHANNEL_ROLE_MANUAL_INGEST, CHANNEL_ROLE_RESOURCE
from app.core.database import AsyncSessionLocal
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.channel import ChannelSetting
from app.monitor.resource_monitor import ResourceMonitor
from app.schemas.telegram_source import TelegramSourceMessage

logger = logging.getLogger(__name__)


async def run_once() -> int:
    """Deliver durable outbox records without opening a Telegram subscription."""

    async def ingest(payload: dict) -> None:
        source = TelegramSourceMessage.model_validate(payload)
        async with AsyncSessionLocal() as db, db.begin():
            setting = await db.scalar(select(ChannelSetting).where(ChannelSetting.channel_id == source.channel_id))
            await ChannelIngestService.process_source_message(db, source, channel_setting=setting)

    monitor = ResourceMonitor(outbox_path=get_settings().resource_messages_db, ingest_handler=ingest)
    return await monitor.deliver_pending()


async def run_resource_monitor(
    *,
    settings: Any | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    monitor_factory: Callable[..., ResourceMonitor] = ResourceMonitor,
) -> None:
    """Run the isolated media monitor for enabled RESOURCE/MANUAL_INGEST channels only."""
    settings = settings or get_settings()
    session_factory = session_factory or AsyncSessionLocal
    if not settings.telegram_api_id or not settings.telegram_api_hash:
        raise RuntimeError('TELEGRAM_API_ID and TELEGRAM_API_HASH are required for the resource monitor')

    async with session_factory() as db:
        rows = list((await db.scalars(
            select(ChannelSetting)
            .where(
                ChannelSetting.enabled.is_(True),
                ChannelSetting.role.in_([CHANNEL_ROLE_RESOURCE, CHANNEL_ROLE_MANUAL_INGEST]),
            )
            .order_by(ChannelSetting.channel_id)
        )).all())
    if not rows:
        logger.warning('resource monitor did not start: no enabled resource or manual-ingest channels')
        return
    settings_by_channel = {row.channel_id: row for row in rows}

    async def ingest(payload: dict) -> None:
        source = TelegramSourceMessage.model_validate(payload)
        async with session_factory() as db, db.begin():
            setting = await db.scalar(select(ChannelSetting).where(ChannelSetting.channel_id == source.channel_id))
            await ChannelIngestService.process_source_message(db, source, channel_setting=setting)

    monitor = monitor_factory(outbox_path=settings.resource_messages_db, ingest_handler=ingest)
    await monitor.run_telethon(
        session=settings.resource_monitor_session,
        api_id=settings.telegram_api_id,
        api_hash=settings.telegram_api_hash,
        channels=list(settings_by_channel),
        settings_by_channel=settings_by_channel,
    )
