import sqlite3

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from migration_tools.import_watchlist import migrate


@pytest.mark.asyncio
async def test_watchlist_migration_report_keeps_unmigrated_rows_visible(tmp_path):
    source = tmp_path / 'legacy_watchlist.db'
    with sqlite3.connect(source) as db:
        db.execute('''CREATE TABLE watchlist (
            tmdb_id INTEGER, title TEXT, season INTEGER, total_episodes INTEGER,
            last_aired_episode INTEGER, collected_episodes TEXT, source TEXT,
            status TEXT, follow_mode TEXT, user_id INTEGER
        )''')
        db.execute('INSERT INTO watchlist VALUES (NULL, ?, 1, NULL, NULL, ?, ?, ?, ?, 7)', ('没有 TMDB 的旧记录', '[]', 'legacy', 'FOLLOWING', 'AUTO'))
        db.commit()
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/target.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    report = await migrate(str(source), session_factory=sessions, report_path=str(tmp_path / 'report.json'))

    assert report['invalid_rows'] == 1
    assert report['unmigrated_rows'] == [{
        'legacy_rowid': 1,
        'reason': 'missing_tmdb_id',
        'title': '没有 TMDB 的旧记录',
    }]
    await engine.dispose()
