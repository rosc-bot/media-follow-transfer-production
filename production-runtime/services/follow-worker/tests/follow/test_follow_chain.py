import sqlite3

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.bot_settings import BotSettings
from app.models.cloud import CloudConfig
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.scout.message_search import MessageSearch
from app.scout.scout_service import ScoutService
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.queue_worker import TransferQueueWorker
from app.transfer.status import TransferOutcome


class FakeAdapter:
    async def transfer(self, payload):
        # Phase 2A verification semantics: a successful transfer reports the files
        # actually observed in the target directory (never an empty success).
        files = tuple(payload.get('expected_files', [])) or ('S01E02.mkv',)
        return TransferOutcome(True, True, 'folder', files)


@pytest.mark.asyncio
async def test_follow_scout_ingest_transfer_and_watchlist_writeback(tmp_path):
    resource_db=tmp_path/'resource_messages.db'
    with sqlite3.connect(resource_db) as db:
        db.execute('CREATE TABLE messages(chat_id TEXT,message_id INTEGER,text TEXT,urls TEXT,chat_title TEXT,date INTEGER)')
        db.execute('INSERT INTO messages VALUES(?,?,?,?,?,?)',('-100',5,'测试剧 S01E02','["https://pan.guangyapan.com/s/x"]','资源频道',1))
        db.commit()
    engine=create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/app.db')
    async with engine.begin() as conn: await conn.run_sync(Base.metadata.create_all)
    sessions=async_sessionmaker(engine,expire_on_commit=False)
    async with sessions() as db:
        async with db.begin():
            db.add_all([
                BotSettings(key="global_pause", val="0"),
                BotSettings(key="transfer_paused", val="0"),
                CloudConfig(name="guangya", auth_ref="test-auth", target_folder_id="completed-root", ongoing_target_folder_id="ongoing-root", enabled=True),
                SeriesWatchlist(tmdb_id=123,title='测试剧',season=1,total_episodes=2,last_aired_episode=2,collected_episodes=[],media_type='tv',status='FOLLOWING',follow_mode='AUTO'),
            ])
        async with db.begin():
            results=await ScoutService(MessageSearch(str(resource_db))).scout_missing(db,tmdb_id=123,title='测试剧',season=1,missing_episodes=['S01E02'])
            assert results[0]['queued'] is True
    worker=TransferQueueWorker(sessions,TransferOrchestrator({'guangya':FakeAdapter()}),worker_id='test')
    assert await worker.process_once() is True
    async with sessions() as db:
        row=await db.scalar(select(SeriesWatchlist).where(SeriesWatchlist.tmdb_id==123))
        task=await db.scalar(select(TransferQueueTask))
        assert task.status == 'COMPLETED'
        assert row.collected_episodes == ['S01E02']
    await engine.dispose()
