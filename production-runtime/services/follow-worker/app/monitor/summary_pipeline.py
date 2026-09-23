"""Summary Message Pipeline for Group Chat Archiving and Telegram Summary."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SUMMARY_SCHEMA = """
CREATE TABLE IF NOT EXISTS chats (
    chat_id INTEGER PRIMARY KEY,
    title TEXT,
    kind TEXT DEFAULT 'group',
    last_seen_at REAL
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    chat_title TEXT,
    message_id INTEGER,
    sender_id INTEGER,
    sender_name TEXT,
    text TEXT,
    date REAL,
    is_reply INTEGER DEFAULT 0,
    reply_to_msg_id INTEGER,
    urls TEXT,
    UNIQUE(chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_chat_date ON messages(chat_id, date);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date);
"""


class SummaryStorage:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self) -> None:
        with self.get_conn() as conn:
            conn.executescript(SUMMARY_SCHEMA)

    def save_group_message(self, chat_id: int, chat_title: str, message: Any, kind: str = "group") -> int | None:
        """First priority: save raw message into tg_messages.db cleanly and immediately."""
        msg_id = getattr(message, "id", None)
        if msg_id is None:
            return None

        sender_id = None
        sender_name = None
        try:
            if getattr(message, "sender_id", None):
                sender_id = message.sender_id
            sender = getattr(message, "sender", None)
            if sender:
                fn = getattr(sender, "first_name", "") or ""
                ln = getattr(sender, "last_name", "") or ""
                un = getattr(sender, "username", "") or ""
                sender_name = f"{fn} {ln}".strip() or un or None
        except Exception:  # noqa: BLE001, S110
            pass

        raw_text = getattr(message, "text", None) or getattr(message, "message", "") or ""
        if not raw_text and getattr(message, "media", None):
            media_cls = message.media.__class__.__name__
            raw_text = f"[媒体消息] {media_cls}"

        date_val = getattr(message, "date", None)
        if isinstance(date_val, (int, float)):
            date_ts = float(date_val)
        elif isinstance(date_val, datetime):
            date_ts = date_val.timestamp()
        else:
            date_ts = time.time()

        is_reply = 1 if getattr(message, "reply_to", None) else 0
        reply_to_msg_id = None
        try:
            if getattr(message, "reply_to", None):
                reply_to_msg_id = getattr(message.reply_to, "reply_to_msg_id", None)
        except Exception:  # noqa: BLE001, S110
            pass

        with self.get_conn() as conn:
            # 1. Upsert chat
            conn.execute(
                """INSERT INTO chats (chat_id, title, kind, last_seen_at) VALUES (?,?,?,?)
                   ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, last_seen_at=excluded.last_seen_at""",
                (chat_id, chat_title or str(chat_id), kind, time.time()),
            )
            # 2. Insert message
            cur = conn.execute(
                """INSERT OR IGNORE INTO messages (
                       chat_id, chat_title, message_id, sender_id, sender_name,
                       text, date, is_reply, reply_to_msg_id, urls
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (chat_id, chat_title, int(msg_id), sender_id, sender_name,
                 raw_text, date_ts, is_reply, reply_to_msg_id, "[]"),
            )
            return cur.lastrowid


async def summary_worker(
    queue: asyncio.Queue,
    storage: SummaryStorage,
    kb_queue: asyncio.Queue | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Worker dedicated to saving raw group chat messages with complete fault isolation."""
    logger.info("SummaryWorker started. Saving group chat messages to %s", storage.db_path)
    while stop_event is None or not stop_event.is_set():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
        except TimeoutError:
            continue
        except asyncio.CancelledError:
            break

        try:
            chat_id = item["chat_id"]
            chat_title = item["chat_title"]
            message = item["message"]
            kind = item.get("kind", "group")

            # Priority 1: Save message in its own isolated transaction
            row_id = storage.save_group_message(chat_id, chat_title, message, kind=kind)

            # If successful and KB queue provided, dispatch to KB queue asynchronously
            if row_id and kb_queue is not None:
                try:
                    kb_queue.put_nowait({
                        "chat_id": chat_id,
                        "chat_title": chat_title,
                        "message_id": getattr(message, "id", None),
                        "sender_name": getattr(getattr(message, "sender", None), "first_name", None),
                        "text": getattr(message, "text", None) or getattr(message, "message", "") or "",
                        "date": getattr(message, "date", None),
                    })
                except Exception as e:  # noqa: BLE001
                    logger.warning("Could not enqueue message to KB worker: %s", e)

        except Exception:
            logger.exception("SummaryWorker failed to save message: %s", item.get("message_id"))
        finally:
            queue.task_done()
