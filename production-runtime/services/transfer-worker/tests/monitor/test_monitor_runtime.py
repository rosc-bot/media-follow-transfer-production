from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.channel import ChannelSetting
from app.monitor.monitor_worker import run_resource_monitor


class RecordingMonitor:
    instance = None

    def __init__(self, *, outbox_path, ingest_handler):
        self.outbox_path = outbox_path
        self.ingest_handler = ingest_handler
        self.called = None
        type(self).instance = self

    async def run_telethon(self, **kwargs):
        self.called = kwargs


@pytest.mark.asyncio
async def test_monitor_runtime_subscribes_only_enabled_media_channels(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/app.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add_all([
            ChannelSetting(channel_id='-101', role='RESOURCE', enabled=True, transfer_mode='AUTO'),
            ChannelSetting(channel_id='-102', role='MANUAL_INGEST', enabled=True, accept_forward=True, transfer_mode='AUTO'),
            ChannelSetting(channel_id='-103', role='RESOURCE', enabled=False, transfer_mode='AUTO'),
        ])
    config = SimpleNamespace(resource_messages_db=str(tmp_path / 'resource_messages.db'), resource_monitor_session='test.session', telegram_api_id=1, telegram_api_hash='hash')

    await run_resource_monitor(settings=config, session_factory=sessions, monitor_factory=RecordingMonitor)

    monitor = RecordingMonitor.instance
    assert monitor.outbox_path == str(tmp_path / 'resource_messages.db')
    assert monitor.called['channels'] == ['-101', '-102']
    assert set(monitor.called['settings_by_channel']) == {'-101', '-102'}
    await engine.dispose()
