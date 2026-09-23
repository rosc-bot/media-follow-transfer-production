"""Read-only internal queries over the live Telegram summary database."""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Annotated, Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query

from app.core.config import get_settings

router = APIRouter(prefix="/internal/telegram", tags=["internal-telegram-readonly"])
_BEIJING = ZoneInfo("Asia/Shanghai")


def _date_bound(value: str, *, upper: bool = False) -> tuple[float, str]:
    raw = str(value or "").strip()
    try:
        if len(raw) == 10:
            day = date.fromisoformat(raw)
            boundary = datetime.combine(day, time.min, tzinfo=_BEIJING)
            if upper:
                return (boundary + timedelta(days=1)).timestamp(), "<"
            return boundary.timestamp(), ">="
        boundary = datetime.fromisoformat(raw)
        if boundary.tzinfo is None:
            boundary = boundary.replace(tzinfo=_BEIJING)
        return boundary.timestamp(), "<=" if upper else ">="
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid Beijing date/time filter") from exc


def _connect_summary_readonly() -> sqlite3.Connection:
    path = Path(get_settings().summary_db_path).expanduser()
    if not path.is_file():
        raise HTTPException(status_code=503, detail="telegram summary database unavailable")
    try:
        connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        return connection
    except sqlite3.Error as exc:
        raise HTTPException(status_code=503, detail="telegram summary database unavailable") from exc


@router.get("/chats")
def list_telegram_chats() -> dict[str, Any]:
    """Return chat metadata and counts from the configured live summary DB."""
    try:
        with _connect_summary_readonly() as db:
            rows = db.execute(
                """SELECT c.chat_id, c.title, c.kind, COUNT(m.id) AS message_count,
                          datetime(MAX(m.date), 'unixepoch', '+8 hours') AS last_message_at_beijing
                   FROM chats AS c
                   LEFT JOIN messages AS m ON m.chat_id = c.chat_id
                   GROUP BY c.chat_id, c.title, c.kind
                   ORDER BY MAX(m.date) DESC, c.title COLLATE NOCASE"""
            ).fetchall()
        return {"source": "SUMMARY_DB_PATH", "chats": [dict(row) for row in rows]}
    except sqlite3.Error as exc:
        raise HTTPException(status_code=503, detail="telegram summary database query failed") from exc


@router.get("/messages")
def list_telegram_messages(
    chat_id: int,
    limit: Annotated[int, Query(ge=1, le=200)] = 20,
    hours: Annotated[int | None, Query(ge=1, le=8760)] = None,
    since: str | None = None,
    until: str | None = None,
    search: str | None = None,
    sender: str | None = None,
) -> dict[str, Any]:
    """Return the exact latest N matching messages, chronologically, in Beijing time."""
    try:
        with _connect_summary_readonly() as db:
            chat = db.execute(
                "SELECT chat_id, title FROM chats WHERE chat_id=?",
                (int(chat_id),),
            ).fetchone()
            if chat is None:
                raise HTTPException(status_code=404, detail="chat_id not found in summary database")
            filters = ["chat_id=?"]
            params: list[Any] = [int(chat_id)]
            if hours is not None:
                filters.append("date>=?")
                params.append(datetime.now(UTC).timestamp() - int(hours) * 3600)
            if since:
                value, operator = _date_bound(since)
                filters.append(f"date{operator}?")
                params.append(value)
            if until:
                value, operator = _date_bound(until, upper=True)
                filters.append(f"date{operator}?")
                params.append(value)
            if search:
                filters.append("instr(lower(COALESCE(text, '')), lower(?))>0")
                params.append(search)
            if sender:
                filters.append("(instr(lower(COALESCE(sender_name, '')), lower(?))>0 OR CAST(sender_id AS TEXT)=?)")
                params.extend([sender, sender])
            params.append(int(limit))
            rows = db.execute(
                """SELECT chat_id, message_id, sender_id, sender_name, text,
                          datetime(date, 'unixepoch', '+8 hours') AS date,
                          reply_to_msg_id AS reply_to
                   FROM messages
                   WHERE """ + " AND ".join(filters) +
                " ORDER BY date DESC, message_id DESC LIMIT ?",
                params,
            ).fetchall()
        messages = [dict(row) for row in reversed(rows)]
        return {
            "source": "SUMMARY_DB_PATH",
            "chat_id": int(chat["chat_id"]),
            "chat_title": chat["title"],
            "limit": int(limit),
            "returned": len(messages),
            "messages": messages,
        }
    except HTTPException:
        raise
    except sqlite3.Error as exc:
        raise HTTPException(status_code=503, detail="telegram summary database query failed") from exc
