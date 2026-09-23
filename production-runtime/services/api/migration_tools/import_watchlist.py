import asyncio
import hashlib
import json
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.database import AsyncSessionLocal
from app.follow.follow_mode import normalize_follow_mode
from app.models.watchlist import SeriesWatchlist

_REQUIRED_COLUMNS = (
    'tmdb_id', 'title', 'season', 'total_episodes', 'last_aired_episode',
    'collected_episodes', 'source', 'status', 'follow_mode', 'user_id',
)


def _source_path(path: str) -> Path:
    source = Path(path).resolve()
    if not source.is_file():
        raise FileNotFoundError(f'legacy watchlist database does not exist: {source}')
    return source


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_rows(path: str) -> list[tuple]:
    source = _source_path(path)
    uri = f'file:{source}?mode=ro'
    with sqlite3.connect(uri, uri=True) as db:
        columns = {row[1] for row in db.execute('PRAGMA table_info(watchlist)')}
        missing = set(_REQUIRED_COLUMNS) - columns
        if missing:
            raise ValueError(f'legacy watchlist schema is missing columns: {sorted(missing)}')
        return db.execute(
            'SELECT rowid,tmdb_id,title,season,total_episodes,last_aired_episode,'
            'collected_episodes,source,status,follow_mode,user_id FROM watchlist ORDER BY rowid'
        ).fetchall()


def _parse_collected(value: object) -> list[str]:
    try:
        result = json.loads(value or '[]')
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return [str(item) for item in result] if isinstance(result, list) else []


def _default_report_path() -> Path:
    stamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
    return Path('data/migration-reports') / f'watchlist-{stamp}.json'


def _write_report(path: str | None, report: dict) -> None:
    output = Path(path) if path else _default_report_path()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + '\n')


async def migrate(
    path: str,
    *,
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionLocal,
    report_path: str | None = None,
) -> dict:
    """Import legacy watchlist data by a read-only URI; never changes the legacy file."""
    source = _source_path(path)
    before = _sha256(source)
    rows = read_rows(str(source))
    created = skipped_existing = invalid_rows = 0
    unmigrated_rows: list[dict] = []
    skipped_existing_rows: list[dict] = []

    async with session_factory() as db, db.begin():
        for raw in rows:
            (legacy_rowid, tmdb_id, title, season, total, last_aired, collected,
             source_name, status, follow_mode, user_id) = raw
            if tmdb_id is None:
                invalid_rows += 1
                unmigrated_rows.append({
                    'legacy_rowid': legacy_rowid,
                    'reason': 'missing_tmdb_id',
                    'title': str(title or ''),
                })
                continue
            try:
                tmdb_id = int(tmdb_id)
                season = int(season or 1)
            except (TypeError, ValueError):
                invalid_rows += 1
                unmigrated_rows.append({
                    'legacy_rowid': legacy_rowid,
                    'reason': 'invalid_tmdb_or_season',
                    'title': str(title or ''),
                })
                continue
            title = str(title or '').strip()
            if tmdb_id <= 0 or season <= 0 or not title:
                invalid_rows += 1
                reason = 'missing_title' if not title else ('invalid_tmdb_id' if tmdb_id <= 0 else 'invalid_season')
                unmigrated_rows.append({
                    'legacy_rowid': legacy_rowid,
                    'reason': reason,
                    'title': title,
                })
                continue
            existing = await db.scalar(select(SeriesWatchlist).where(
                SeriesWatchlist.tmdb_id == tmdb_id,
                SeriesWatchlist.season == season,
                SeriesWatchlist.subscriber_tg_id == user_id,
            ))
            if existing:
                skipped_existing += 1
                skipped_existing_rows.append({
                    'legacy_rowid': legacy_rowid,
                    'reason': 'existing_watchlist_identity',
                    'title': title,
                    'tmdb_id': tmdb_id,
                    'season': season,
                    'subscriber_tg_id': user_id,
                })
                continue
            db.add(SeriesWatchlist(
                tmdb_id=tmdb_id,
                title=title,
                season=season,
                total_episodes=total,
                last_aired_episode=last_aired,
                collected_episodes=_parse_collected(collected),
                source=source_name,
                status=status or 'FOLLOWING',
                follow_mode=normalize_follow_mode(follow_mode),
                subscriber_tg_id=user_id,
                media_type='tv',
            ))
            created += 1

    after = _sha256(source)
    report = {
        'migration': 'watchlist',
        'source_path': str(source),
        'source_rows': len(rows),
        'created': created,
        'skipped_existing': skipped_existing,
        'invalid_rows': invalid_rows,
        'unmigrated_rows': unmigrated_rows,
        'skipped_existing_rows': skipped_existing_rows,
        'source_sha256_before': before,
        'source_sha256_after': after,
        'source_unchanged': before == after,
    }
    _write_report(report_path, report)
    return report


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('--report')
    args = parser.parse_args()
    print(json.dumps(asyncio.run(migrate(args.path, report_path=args.report)), ensure_ascii=False))
