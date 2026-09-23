import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from app.api import telegram as telegram_api
from app.api.app import app


def _make_summary_db(path):
    with sqlite3.connect(path) as db:
        db.executescript(
            """
            CREATE TABLE chats(chat_id INTEGER PRIMARY KEY, title TEXT, kind TEXT, last_seen_at REAL);
            CREATE TABLE messages(
                id INTEGER PRIMARY KEY, chat_id INTEGER NOT NULL, chat_title TEXT, message_id INTEGER,
                sender_id INTEGER, sender_name TEXT, text TEXT, date REAL, is_reply INTEGER, reply_to_msg_id INTEGER, urls TEXT
            );
            INSERT INTO chats VALUES(-100123, '测试群', 'group', 1790180000);
            INSERT INTO messages VALUES(1,-100123,'测试群',10,101,'甲','旧消息',1790179000,0,NULL,'[]');
            INSERT INTO messages VALUES(2,-100123,'测试群',11,202,'乙','最近消息一',1790179100,1,10,'[]');
            INSERT INTO messages VALUES(3,-100123,'测试群',12,303,'丙','最近消息二',1790179200,0,NULL,'[]');
            """
        )


@pytest.mark.asyncio
async def test_internal_chats_endpoint_reads_summary_db_readonly(tmp_path, monkeypatch):
    db_path = tmp_path / "tg_messages.db"
    _make_summary_db(db_path)
    monkeypatch.setattr(telegram_api, "get_settings", lambda: SimpleNamespace(summary_db_path=str(db_path)))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/internal/telegram/chats")

    assert response.status_code == 200
    body = response.json()
    assert body["chats"][0]["chat_id"] == -100123
    assert body["chats"][0]["message_count"] == 3
    assert body["chats"][0]["last_message_at_beijing"] == "2026-09-24 00:00:00"
    with sqlite3.connect(db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 3


@pytest.mark.asyncio
async def test_internal_messages_endpoint_returns_exact_latest_count_and_sender_ids(tmp_path, monkeypatch):
    db_path = tmp_path / "tg_messages.db"
    _make_summary_db(db_path)
    monkeypatch.setattr(telegram_api, "get_settings", lambda: SimpleNamespace(summary_db_path=str(db_path)))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/internal/telegram/messages", params={"chat_id": -100123, "limit": 2})

    assert response.status_code == 200
    body = response.json()
    assert body["returned"] == 2
    assert [item["message_id"] for item in body["messages"]] == [11, 12]
    assert [item["sender_id"] for item in body["messages"]] == [202, 303]
    assert body["messages"][0]["reply_to"] == 10
    assert body["messages"][0]["date"] == "2026-09-23 23:58:20"
    assert body["messages"][1]["date"] == "2026-09-24 00:00:00"


@pytest.mark.asyncio
async def test_internal_messages_query_failure_is_not_reported_as_empty(tmp_path, monkeypatch):
    missing = tmp_path / "missing.db"
    monkeypatch.setattr(telegram_api, "get_settings", lambda: SimpleNamespace(summary_db_path=str(missing)))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/internal/telegram/messages", params={"chat_id": -100123, "limit": 20})

    assert response.status_code == 503
    assert response.json()["detail"] == "telegram summary database unavailable"
