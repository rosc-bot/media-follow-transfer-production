"""Unified Telegram Gateway: Single Client, Session Reuse, Demuxed Event Workers."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.database import AsyncSessionLocal
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.channel import ChannelSetting
from app.monitor.event_router import EventRouter
from app.monitor.kb_pipeline import KBStorage, kb_worker
from app.monitor.resource_pipeline import (
    ResourceStorage,
    outbox_retry_loop,
    resource_worker,
)
from app.monitor.summary_pipeline import SummaryStorage, summary_worker
from app.schemas.telegram_source import TelegramSourceMessage

logger = logging.getLogger(__name__)


class TelegramGateway:
    def __init__(
        self,
        *,
        session_path: str,
        api_id: int,
        api_hash: str,
        summary_db_path: str,
        resource_db_path: str,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ):
        self.session_path = session_path
        self.api_id = api_id
        self.api_hash = api_hash
        self.summary_db_path = summary_db_path
        self.resource_db_path = resource_db_path
        self.session_factory = session_factory or AsyncSessionLocal

        # Isolated internal queues
        self.summary_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.resource_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.kb_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

        # Isolated storage managers
        self.summary_storage = SummaryStorage(summary_db_path)
        self.kb_storage = KBStorage(summary_db_path)
        self.resource_storage = ResourceStorage(resource_db_path)

        # Event Router
        self.router = EventRouter(self.summary_queue, self.resource_queue)

        self.stop_event = asyncio.Event()
        self.client: Any = None
        self.tasks: list[asyncio.Task] = []

    async def ingest_handler(self, payload: dict) -> None:
        """Isolated ingest handler delivering to ChannelIngestService."""
        source = TelegramSourceMessage.model_validate(payload)
        async with self.session_factory() as db, db.begin():
            setting = await db.scalar(select(ChannelSetting).where(ChannelSetting.channel_id == source.channel_id))
            await ChannelIngestService.process_source_message(db, source, channel_setting=setting)

    async def load_channel_settings(self) -> dict[str, Any]:
        async with self.session_factory() as db:
            rows = list((await db.scalars(
                select(ChannelSetting).where(ChannelSetting.enabled.is_(True))
            )).all())
        return {row.channel_id: row for row in rows}

    async def start(self) -> None:
        """Start the single long-lived TelegramClient and background isolated workers."""
        from telethon import TelegramClient, events

        # Load channel settings from database
        settings_map = await self.load_channel_settings()
        self.router.update_settings(settings_map)
        logger.info("Loaded %d enabled channel/group settings for router", len(settings_map))

        # Start 4 isolated background workers
        self.tasks.append(asyncio.create_task(
            summary_worker(self.summary_queue, self.summary_storage, kb_queue=self.kb_queue, stop_event=self.stop_event)
        ))
        self.tasks.append(asyncio.create_task(
            kb_worker(self.kb_queue, self.kb_storage, stop_event=self.stop_event)
        ))
        self.tasks.append(asyncio.create_task(
            resource_worker(self.resource_queue, self.resource_storage, ingest_handler=self.ingest_handler, stop_event=self.stop_event)
        ))
        self.tasks.append(asyncio.create_task(
            outbox_retry_loop(self.resource_storage, ingest_handler=self.ingest_handler, interval_seconds=30.0, stop_event=self.stop_event)
        ))

        # Single TelegramClient with single persistent session
        logger.info("Initializing TelegramClient with session: %s", self.session_path)
        self.client = TelegramClient(self.session_path, self.api_id, self.api_hash)

        @self.client.on(events.NewMessage())
        async def on_new_message(event):
            await self.router.route_event(event, client=self.client)

        await self.client.start()
        me = await self.client.get_me()
        logger.info("TelegramClient connected successfully as %s (@%s)", getattr(me, "first_name", ""), getattr(me, "username", ""))
        await self.client.run_until_disconnected()

    async def stop(self) -> None:
        self.stop_event.set()
        for t in self.tasks:
            t.cancel()
        if self.client and self.client.is_connected():
            await self.client.disconnect()
        logger.info("TelegramGateway stopped.")


async def run_gateway() -> None:
    settings = get_settings()
    if not settings.telegram_api_id or not settings.telegram_api_hash:
        raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required for TelegramGateway")

    gateway = TelegramGateway(
        session_path=settings.tg_session_path,
        api_id=settings.telegram_api_id,
        api_hash=settings.telegram_api_hash,
        summary_db_path=settings.summary_db_path,
        resource_db_path=settings.resource_db_path,
    )
    await gateway.start()
