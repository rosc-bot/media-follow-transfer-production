import json
import sqlite3
from hashlib import sha256

import pytest
from sqlalchemy import BigInteger, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.watchlist import SeriesWatchlist
from migration_tools.import_watchlist import migrate


@pytest.mark.asyncio
async def test_watchlist_migration_is_read_only_idempotent_and_writes_report(tmp_path):
    source = tmp_path / 'legacy_watchlist.db'
    with sqlite3.connect(source) as db:
        db.execute('''CREATE TABLE watchlist (
            tmdb_id INTEGER, title TEXT, season INTEGER, total_episodes INTEGER,
            last_aired_episode INTEGER, collected_episodes TEXT, source TEXT,
            status TEXT, follow_mode TEXT, user_id INTEGER
        )''')
        db.execute(
            'INSERT INTO watchlist VALUES (99, ?, 1, 4, 2, ?, ?, ?, ?, ?)',
            ('迁移剧', '["S01E01"]', 'legacy', 'FOLLOWING', 'AUTO', 8586984520),
        )
        db.commit()
    source_hash = sha256(source.read_bytes()).hexdigest()
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/target.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    first_report_path = tmp_path / 'first-report.json'
    second_report_path = tmp_path / 'second-report.json'

    first = await migrate(str(source), session_factory=sessions, report_path=str(first_report_path))
    second = await migrate(str(source), session_factory=sessions, report_path=str(second_report_path))

    async with sessions() as db:
        count = await db.scalar(select(func.count()).select_from(SeriesWatchlist))
    assert count == 1
    assert isinstance(SeriesWatchlist.__table__.c.subscriber_tg_id.type, BigInteger)
    assert source_hash == sha256(source.read_bytes()).hexdigest()
    assert first['source_unchanged'] is True and first['created'] == 1 and first['skipped_existing'] == 0
    assert second['source_unchanged'] is True and second['created'] == 0 and second['skipped_existing'] == 1
    assert json.loads(second_report_path.read_text()) == second
    await engine.dispose()
