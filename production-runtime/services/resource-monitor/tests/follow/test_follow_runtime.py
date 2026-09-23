import sqlite3

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.follow.calendar_service import CalendarService
from app.follow.follow_worker import run_follow_cycle
from app.models.bot_settings import BotSettings
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.scout.message_search import MessageSearch
from app.scout.scout_service import ScoutService


class FakeCalendar:
    async def fetch_schedule(self, tmdb_id, season):
        assert (tmdb_id, season) == (321, 1)
        return {'total_episodes': 2, 'last_aired_episode': 2}


@pytest.mark.asyncio
async def test_follow_cycle_syncs_tmdb_schedule_then_scouts_missing_episodes(tmp_path):
    resource_db = tmp_path / 'resource_messages.db'
    with sqlite3.connect(resource_db) as db:
        db.execute('CREATE TABLE messages(chat_id TEXT,message_id INTEGER,text TEXT,urls TEXT,chat_title TEXT,date INTEGER)')
        db.execute('INSERT INTO messages VALUES(?,?,?,?,?,?)', ('-100', 5, '循环剧 S01E02', '["https://pan.guangyapan.com/s/cycle"]', '资源频道', 1))
        db.commit()
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/app.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="0"),
            BotSettings(key="follow_paused", val="0"),
            SeriesWatchlist(tmdb_id=321, title='循环剧', season=1, collected_episodes=[], status='FOLLOWING'),
        ])
    async with sessions() as db, db.begin():
        result = await run_follow_cycle(
            db,
            calendar=CalendarService(FakeCalendar()),
            scout=ScoutService(MessageSearch(str(resource_db))),
        )
    async with sessions() as db:
        watchlist = await db.scalar(select(SeriesWatchlist).where(SeriesWatchlist.tmdb_id == 321))
        task = await db.scalar(select(TransferQueueTask))
        assert result['synced_watchlists'] == 1
        # New Phase 2B semantics: every processed episode counts as a scout
        # job (hits AND misses are both reported for explainability).
        assert result['scout_jobs'] == 2
        assert result['cycle_stats']['target_episodes'] == 2
        assert result['cycle_stats']['local_hit'] == 1
        assert watchlist.last_aired_episode == 2
        assert task.payload['episode_keys'] == ['S01E02']
    await engine.dispose()
