import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.channel import ChannelSetting
from app.models.ingest import ChannelIngestJob, ChannelIngestMessage
from app.models.transfer import TransferQueueTask
from app.schemas.telegram_source import TelegramSourceMessage


@pytest.mark.asyncio
async def test_manual_forward_keeps_flag_through_ingest_and_queue(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/ingest.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    setting = ChannelSetting(channel_id='test', role='MANUAL_INGEST', accept_forward=True, transfer_mode='AUTO')
    source = TelegramSourceMessage(source_type='manual_forward', channel_id='test', message_id=11,
        text='测试剧 S01E02 https://pan.guangyapan.com/s/demo', is_forward=True,
        metadata={'tmdb_id': 123, 'title': '测试剧', 'file_names': ['测试剧.S01E02.mkv']})
    async with sessions() as db:
        async with db.begin():
            result = await ChannelIngestService.process_source_message(db, source, channel_setting=setting)
            assert result['queued'] is True
        job = await db.scalar(select(ChannelIngestJob).where(ChannelIngestJob.id == result['job_id']))
        msg = await db.scalar(select(ChannelIngestMessage).where(ChannelIngestMessage.message_id == 11))
        task = await db.scalar(select(TransferQueueTask))
        assert job.is_forward is True
        assert job.parsed_data['is_forward'] is True
        assert msg.is_forward is True
        assert task.payload['is_forward'] is True
    await engine.dispose()


@pytest.mark.asyncio
async def test_manual_forward_without_forward_marker_is_rejected(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/ingest.db')
    async with engine.begin() as conn: await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    setting = ChannelSetting(channel_id='test', role='MANUAL_INGEST', accept_forward=True, transfer_mode='AUTO')
    source = TelegramSourceMessage(source_type='manual_forward', channel_id='test', message_id=12, is_forward=False, text='x')
    async with sessions() as db, db.begin():
        result = await ChannelIngestService.process_source_message(db, source, channel_setting=setting)
        assert result['status'] == 'NEEDS_REVIEW'
    await engine.dispose()
