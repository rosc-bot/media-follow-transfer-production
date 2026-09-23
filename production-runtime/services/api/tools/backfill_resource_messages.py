"""Backfill resource messages from tg_messages.db into resource_messages.db.

Phase 2B: restore the historical resource index without widening Scout to the
whole 130k-row chat database.

Rules
-----
* Only channels present in the authoritative whitelist are processed.  The
  whitelist comes from ``channel_settings`` (PostgreSQL) where
  ``enabled AND role IN (RESOURCE, MANUAL_INGEST)``, or from explicit
  ``--channel-ids`` (comma separated) when no database is reachable.
* Idempotent on (chat_id, message_id) — repeated runs never duplicate rows.
* Messages without a supported share URL are counted (no_share_url) but are
  NOT inserted, so chat noise can never become a Scout candidate.
* Extended columns (sender_id, sender_name, forward_info, source_link) are
  added to the existing messages table with a compatible ALTER — the existing
  427 rows are preserved untouched.

Usage
-----
  python -m tools.backfill_resource_messages --database-url "$DATABASE_URL" --dry-run
  python -m tools.backfill_resource_messages --database-url "$DATABASE_URL" --apply
  python -m tools.backfill_resource_messages --channel-ids 3702243011,3667471790 --source /tmp/tg_messages.db --target /tmp/resource_messages.db --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

from app.ingest.resource_link_extractor import ResourceLinkExtractor

# --------------------------------------------------------------------------- #
# ID normalization: tg_messages.db stores full ids ("-1003702243011") while
# resource_messages.db stores the short form ("3702243011").
# --------------------------------------------------------------------------- #

def full_to_short(raw: str) -> str:
    value = str(raw).strip()
    if value.startswith('-100') and len(value) > 4:
        return value[4:]
    return value.lstrip('-')


def normalize_chat_id(raw: object) -> str:
    return full_to_short(str(raw or ''))


# --------------------------------------------------------------------------- #
# Compatible schema extension (never drops columns).
# --------------------------------------------------------------------------- #

EXTENDED_COLUMNS: dict[str, str] = {
    'sender_id': 'INTEGER',
    'sender_name': 'TEXT',
    'forward_info': 'TEXT',
    'source_link': 'TEXT',
}

_RESOURCE_MESSAGES_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT NOT NULL,
    chat_title TEXT,
    message_id INTEGER NOT NULL,
    text TEXT NOT NULL,
    urls TEXT NOT NULL DEFAULT '[]',
    source_type TEXT NOT NULL,
    is_forward INTEGER NOT NULL DEFAULT 0,
    date TEXT,
    UNIQUE(chat_id, message_id)
);
"""


def ensure_schema(db_path: str) -> None:
    """Create the messages table if missing and add extended columns if absent."""
    with sqlite3.connect(db_path) as conn:
        conn.executescript(_RESOURCE_MESSAGES_SCHEMA)
        existing = {row[1] for row in conn.execute('PRAGMA table_info(messages)').fetchall()}
        for column, ctype in EXTENDED_COLUMNS.items():
            if column not in existing:
                conn.execute(f'ALTER TABLE messages ADD COLUMN "{column}" {ctype}')


# --------------------------------------------------------------------------- #
# Whitelist resolution
# --------------------------------------------------------------------------- #

def resolve_whitelist(database_url: str | None, explicit_ids: list[str]) -> list[str]:
    """Return short-form scout-allowed chat ids."""
    if explicit_ids:
        return [normalize_chat_id(value) for value in explicit_ids]
    if not database_url:
        raise SystemExit('must provide --database-url or --channel-ids')
    import asyncio

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    async def _load() -> list[str]:
        engine = create_async_engine(database_url, pool_pre_ping=True)
        try:
            async with engine.connect() as connection:
                rows = (await connection.execute(text(
                    "SELECT channel_id FROM channel_settings "
                    "WHERE enabled = TRUE AND role IN ('RESOURCE', 'MANUAL_INGEST') "
                    "ORDER BY channel_id"
                ))).mappings().all()
                return [normalize_chat_id(row['channel_id']) for row in rows]
        finally:
            await engine.dispose()

    return asyncio.run(_load())


# --------------------------------------------------------------------------- #
# Core backfill
# --------------------------------------------------------------------------- #

def _extract_urls_block(text: str) -> list[str]:
    """Simple URL extraction for plain-text rows (tg_messages.db stores '[]')."""
    urls: list[str] = []
    line = (text or '') + ' '
    start = 0
    while True:
        index = line.find('http', start)
        if index == -1:
            break
        end = index
        while end < len(line) and line[end] not in ' \n\t\u3000':
            end += 1
        candidate = line[index:end]
        candidate = re.sub(r'^[\s"\'<\[{(\u201c\u2018]+', '', candidate)
        candidate = re.sub(r'[\s.,;:!?)"\]]}>,。；：！？）】》\u201d\u2019]+$', '', candidate)
        if candidate.startswith(('http://', 'https://')):
            urls.append(candidate)
        start = end
    return urls


def backfill(
    *,
    source_db: str,
    target_db: str,
    whitelist: list[str],
    apply: bool,
) -> dict:
    """Scan tg_messages db and report per-channel stats; insert when apply=True."""
    allowed = set(whitelist)
    ensure_schema(target_db)
    # Source is a live SQLite file being written by the monitor; open it
    # read-only so the backfill can never touch its WAL/locks.
    source_uri = f'file:{Path(source_db).resolve()}?mode=ro'
    with sqlite3.connect(source_uri, uri=True) as src:
        src.row_factory = sqlite3.Row
        rows = src.execute(
            'SELECT chat_id, chat_title, message_id, sender_id, sender_name, text, date, is_reply, urls '
            'FROM messages ORDER BY chat_id, message_id'
        ).fetchall()

    with sqlite3.connect(target_db) as dst:
        existing = {
            (str(row[0]), int(row[1]))
            for row in dst.execute('SELECT chat_id, message_id FROM messages').fetchall()
        }
        insert_many: list[tuple] = []
        stats: dict[str, dict] = {}
        skipped_unsupported: dict[str, int] = {}
        total_candidates = 0

        for row in rows:
            chat_id = normalize_chat_id(row['chat_id'])
            if chat_id not in allowed:
                continue
            message_id = int(row['message_id'])
            key = chat_id
            stats.setdefault(key, {
                'chat_id': chat_id,
                'chat_title': row['chat_title'] or '',
                'total_messages': 0,
                'with_share_url': 0,
                'no_share_url': 0,
                'parse_failed': 0,
                'already_exists': 0,
                'to_add': 0,
            })
            stats[key]['total_messages'] += 1

            if (chat_id, message_id) in existing:
                stats[key]['already_exists'] += 1
                continue

            urls = _extract_urls_block(row['text'] or '')
            if not urls:
                try:
                    raw_urls = json.loads(row['urls'] or '[]')
                    urls = [str(u) for u in raw_urls if str(u).startswith('http')]
                except json.JSONDecodeError:
                    stats[key]['parse_failed'] += 1

            if not ResourceLinkExtractor.has_supported_share_url(urls):
                stats[key]['no_share_url'] += 1
                skipped_unsupported[key] = skipped_unsupported.get(key, 0) + 1
                continue

            stats[key]['with_share_url'] += 1
            stats[key]['to_add'] += 1
            total_candidates += 1
            insert_many.append((
                chat_id,
                row['chat_title'] or '',
                message_id,
                row['text'] or '',
                json.dumps(urls, ensure_ascii=False),
                'telegram_channel',
                int(bool(row['is_reply'])),
                '',
                row['sender_id'],
                row['sender_name'] or '',
                json.dumps({'forward': False}),
                f'https://t.me/c/{chat_id}/{message_id}',
            ))

        if apply and insert_many:
            dst.executemany(
                'INSERT OR IGNORE INTO messages '
                '(chat_id, chat_title, message_id, text, urls, source_type, is_forward, date, '
                ' sender_id, sender_name, forward_info, source_link) '
                'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                insert_many,
            )
            dst.commit()

    return {
        'mode': 'apply' if apply else 'dry-run',
        'allowed_channels': sorted(allowed),
        'total_candidate_rows': total_candidates,
        'channels': sorted(stats.values(), key=lambda item: item['total_messages'], reverse=True),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description='Backfill resource messages (idempotent).')
    parser.add_argument('--database-url', help='PostgreSQL URL for channel whitelist')
    parser.add_argument('--channel-ids', help='comma separated short chat ids (overrides DB)')
    parser.add_argument('--source', default='./data/tg_messages.db')
    parser.add_argument('--target', default='./data/resource_messages.db')
    parser.add_argument('--dry-run', action='store_true', default=False)
    parser.add_argument('--apply', action='store_true', default=False)
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.error('must specify --dry-run or --apply')
    explicit = [value for value in (args.channel_ids or '').split(',') if value.strip()]
    whitelist = resolve_whitelist(args.database_url, explicit)
    report = backfill(
        source_db=args.source,
        target_db=args.target,
        whitelist=whitelist,
        apply=args.apply,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    sys.exit(0)


if __name__ == '__main__':
    main()
