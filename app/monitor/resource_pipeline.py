"""Resource Message Pipeline for Dedicated Media Channel Ingestion & Durable Outbox."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.constants import (
    CHANNEL_ROLE_MANUAL_INGEST,
    SOURCE_MANUAL_FORWARD,
    SOURCE_TELEGRAM_CHANNEL,
)
from app.ingest.url_extractor import extract_all_urls
from app.schemas.telegram_source import TelegramSourceMessage

logger = logging.getLogger(__name__)

RESOURCE_SCHEMA = """
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
CREATE TABLE IF NOT EXISTS resource_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    sent_at TEXT,
    UNIQUE(channel_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_resource_outbox_due ON resource_outbox(status, next_retry_at, id);
"""


class ResourceStorage:
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
            conn.executescript(RESOURCE_SCHEMA)

    def save_and_enqueue(self, payload: dict) -> bool:
        with self.get_conn() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO messages(chat_id, chat_title, message_id, text, urls, source_type, is_forward, date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload["channel_id"],
                    payload.get("channel_title"),
                    payload["message_id"],
                    payload.get("text") or payload.get("caption") or "",
                    json.dumps(payload.get("urls") or [], ensure_ascii=False),
                    payload.get("source_type", SOURCE_TELEGRAM_CHANNEL),
                    int(bool(payload.get("is_forward"))),
                    payload.get("published_at"),
                ),
            )
            cur = conn.execute(
                """INSERT OR IGNORE INTO resource_outbox(channel_id, message_id, payload, status, created_at)
                   VALUES (?, ?, ?, 'PENDING', ?)""",
                (
                    payload["channel_id"],
                    payload["message_id"],
                    json.dumps(payload, ensure_ascii=False),
                    datetime.now(UTC).isoformat(),
                ),
            )
            return cur.rowcount == 1

    def get_pending(self, limit: int = 50) -> list[dict]:
        with self.get_conn() as conn:
            rows = conn.execute(
                "SELECT id, payload FROM resource_outbox WHERE status IN ('PENDING', 'RETRY', 'RETRY_WAIT') ORDER BY id LIMIT ?",
                (limit,),
            ).fetchall()
        return [{"id": row[0], **json.loads(row[1])} for row in rows]

    def mark_sent(self, row_id: int) -> None:
        with self.get_conn() as conn:
            conn.execute(
                "UPDATE resource_outbox SET status='SENT', sent_at=? WHERE id=?",
                (datetime.now(UTC).isoformat(), row_id),
            )

    def mark_retry(self, row_id: int, error: str) -> None:
        with self.get_conn() as conn:
            conn.execute(
                "UPDATE resource_outbox SET status='RETRY_WAIT', attempt_count=attempt_count+1, last_error=? WHERE id=?",
                (error[:1000], row_id),
            )


def serialize_resource_message(message: Any, chat: Any = None, setting: Any = None) -> dict | None:
    """Serializes Telethon message with complete Forward inspection and URL extraction."""
    role = getattr(setting, "role", None)
    accept_forward = bool(getattr(setting, "accept_forward", False))

    # Detailed forward checks across Telethon fields
    is_fwd = bool(
        getattr(message, "fwd_from", None)
        or getattr(message, "forward", None)
        or getattr(message, "forward_date", None)
        or getattr(message, "forward_origin", None)
    )

    if role == CHANNEL_ROLE_MANUAL_INGEST:
        if accept_forward and not is_fwd:
            # Manual ingest channel only accepts forwarded messages
            logger.info("Ignoring non-forward message %s in manual ingest channel", getattr(message, "id", None))
            return None
        source_type = SOURCE_MANUAL_FORWARD
        is_forward_final = True
    else:
        source_type = SOURCE_TELEGRAM_CHANNEL
        is_forward_final = is_fwd

    chat_id = str(getattr(chat, "id", None) or getattr(message, "chat_id", "") or "")
    channel_username = getattr(chat, "username", None) or getattr(getattr(message, "chat", None), "username", None)
    channel_title = str(getattr(chat, "title", None) or getattr(chat, "first_name", None) or "")

    raw_text = getattr(message, "text", None) or getattr(message, "message", "") or ""
    is_media = bool(getattr(message, "media", None))
    text_val = "" if is_media else raw_text
    caption_val = raw_text if is_media else ""

    urls_dict = extract_all_urls(message)
    urls = urls_dict.get("all_urls", [])
    button_urls = urls_dict.get("button_urls", [])

    entities_list = []
    for ent in getattr(message, "entities", None) or []:
        cls_name = ent.__class__.__name__
        entities_list.append({
            "type": "text_link" if cls_name == "MessageEntityTextUrl" else cls_name,
            "offset": int(getattr(ent, "offset", 0)),
            "length": int(getattr(ent, "length", 0)),
            "url": getattr(ent, "url", None),
            "is_caption": is_media,
        })

    date_val = getattr(message, "date", None)
    if hasattr(date_val, "isoformat"):
        pub_at = date_val.isoformat()
    elif isinstance(date_val, (int, float)):
        pub_at = datetime.fromtimestamp(date_val, UTC).isoformat()
    else:
        pub_at = datetime.now(UTC).isoformat()

    source = TelegramSourceMessage(
        source_type=source_type,
        channel_id=chat_id,
        channel_username=channel_username,
        channel_title=channel_title,
        message_id=int(getattr(message, "id", 0)),
        text=text_val,
        caption=caption_val,
        entities=entities_list,
        urls=urls,
        button_urls=button_urls,
        published_at=datetime.fromisoformat(pub_at) if pub_at else datetime.now(UTC),
        is_forward=is_forward_final,
    )
    return source.model_dump(mode="json")


async def resource_worker(
    queue: asyncio.Queue,
    storage: ResourceStorage,
    ingest_handler: Callable[[dict], Any] | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Worker dedicated to resource channel messages with reliable outbox persistence."""
    logger.info("ResourceWorker started. Saving resource messages to %s", storage.db_path)
    while stop_event is None or not stop_event.is_set():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
        except TimeoutError:
            continue
        except asyncio.CancelledError:
            break

        try:
            message = item["message"]
            chat = item["chat"]
            setting = item.get("setting")

            payload = serialize_resource_message(message, chat, setting)
            if payload is None:
                continue

            # 1. Commit transaction in resource_messages.db
            storage.save_and_enqueue(payload)

            # 2. Trigger immediate delivery attempt
            if ingest_handler is not None:
                await deliver_outbox_pending(storage, ingest_handler, limit=10)

        except Exception:
            logger.exception("ResourceWorker error processing resource message: %s", item.get("message_id"))
        finally:
            queue.task_done()


async def deliver_outbox_pending(storage: ResourceStorage, ingest_handler: Callable[[dict], Any], limit: int = 50) -> int:
    """Delivers pending items to ingest_handler; on failure marks retry without crashing."""
    delivered = 0
    items = storage.get_pending(limit=limit)
    for item in items:
        row_id = item.pop("id")
        try:
            await ingest_handler(item)
            storage.mark_sent(row_id)
            delivered += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("Resource outbox delivery failed for row %s: %s", row_id, exc)
            storage.mark_retry(row_id, str(exc))
    return delivered


async def outbox_retry_loop(
    storage: ResourceStorage,
    ingest_handler: Callable[[dict], Any],
    interval_seconds: float = 30.0,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Background loop ensuring reliable retries of pending outbox deliveries."""
    logger.info("OutboxRetryLoop started. Polling every %s seconds.", interval_seconds)
    while stop_event is None or not stop_event.is_set():
        try:
            await deliver_outbox_pending(storage, ingest_handler, limit=50)
        except Exception:
            logger.exception("OutboxRetryLoop error during delivery poll")
        try:
            await asyncio.wait_for(stop_event.wait() if stop_event else asyncio.sleep(interval_seconds), timeout=interval_seconds)
        except (TimeoutError, asyncio.CancelledError):
            continue
