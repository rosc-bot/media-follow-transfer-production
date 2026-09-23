from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.bot_settings import BotSettings
from app.models.channel import ChannelSetting
from app.models.cloud import CloudConfig
from app.models.ingest import ChannelIngestJob
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.monitor.resource_monitor import ResourceMonitor
from app.schemas.telegram_source import TelegramSourceMessage
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.queue_worker import TransferQueueWorker
from app.transfer.status import TransferOutcome


class FakeAdapter:
    async def transfer(self, payload):
        # Phase 2A verification semantics: a successful transfer reports the files
        # actually observed in the target directory (never an empty success).
        files = tuple(payload.get('expected_files', [])) or ('manual-episode.mkv',)
        return TransferOutcome(True, True, 'manual-folder', files)


@pytest.mark.asyncio
async def test_manual_forward_monitor_to_transfer_e2e(tmp_path):
    engine=create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/app.db')
    async with engine.begin() as conn: await conn.run_sync(Base.metadata.create_all)
    sessions=async_sessionmaker(engine,expire_on_commit=False)
    setting=ChannelSetting(channel_id='-8',role='MANUAL_INGEST',enabled=True,accept_forward=True,transfer_mode='AUTO')
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="0"),
            BotSettings(key="transfer_paused", val="0"),
            CloudConfig(name="guangya", auth_ref="test-auth", target_folder_id="completed-root", ongoing_target_folder_id="ongoing-root", enabled=True),
            SeriesWatchlist(tmdb_id=8008, title='人工剧', season=1, subscriber_tg_id=1),
        ])
    message=SimpleNamespace(id=8,chat_id=-8,text='人工剧 S01E01 https://pan.guangyapan.com/s/manual',message='人工剧 S01E01 https://pan.guangyapan.com/s/manual',media=None,fwd_from=SimpleNamespace(),forward=None,forward_date=None,entities=None,buttons=None,date=None)
    chat=SimpleNamespace(id=-8,username='test',title='测试频道')
    monitor=ResourceMonitor(outbox_path=str(tmp_path/'resource_messages.db'))
    payload=monitor.capture(message,chat,setting)
    assert payload and payload['is_forward'] is True and payload['source_type']=='manual_forward'
    async def handler(data):
        async with sessions() as db, db.begin():
            await ChannelIngestService.process_source_message(db,TelegramSourceMessage.model_validate(data),channel_setting=setting)
    monitor.ingest_handler=handler
    assert await monitor.deliver_pending()==1
    worker=TransferQueueWorker(sessions,TransferOrchestrator({'guangya':FakeAdapter()}),worker_id='manual-test')
    assert await worker.process_once()
    async with sessions() as db:
        job=await db.scalar(select(ChannelIngestJob).where(ChannelIngestJob.message_id==8))
        task=await db.scalar(select(TransferQueueTask))
        assert job.is_forward is True and job.parsed_data['is_forward'] is True
        assert task.status=='COMPLETED' and task.payload['is_forward'] is True
    await engine.dispose()
