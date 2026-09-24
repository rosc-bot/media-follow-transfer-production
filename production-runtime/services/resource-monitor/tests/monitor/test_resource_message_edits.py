import json
import sqlite3
from datetime import UTC, datetime

from app.monitor.resource_pipeline import ResourceStorage
from app.schemas.telegram_source import TelegramSourceMessage


def _payload(*, text="", caption="", urls=None):
    return {
        "channel_id": "4429917555",
        "channel_title": "光鸭云盘资源频道",
        "message_id": 28157,
        "text": text,
        "caption": caption,
        "urls": urls or [],
        "button_urls": [],
        "entities": [],
        "source_type": "telegram_channel",
        "is_forward": False,
        "published_at": datetime(2026, 9, 23, tzinfo=UTC).isoformat(),
    }


def test_edited_message_upserts_row_and_reopens_outbox_only_for_changed_hash(tmp_path):
    storage = ResourceStorage(str(tmp_path / "resource_messages.db"))

    assert storage.save_and_enqueue(_payload(caption="测试剧 S02E01")) is True
    with storage.get_conn() as db:
        db.execute("UPDATE resource_outbox SET status='SENT', sent_at='sent' WHERE message_id=28157")
        first = db.execute("SELECT content_hash, updated_at FROM messages WHERE message_id=28157").fetchone()
        assert first["content_hash"]

    assert storage.save_and_enqueue(_payload(caption="测试剧 S02E01")) is False
    with storage.get_conn() as db:
        unchanged = db.execute("SELECT status FROM resource_outbox WHERE message_id=28157").fetchone()
        assert unchanged["status"] == "SENT"

    edited = _payload(caption="测试剧 S02E01-S02E02", urls=["https://pan.guangyapan.com/s/updated"])
    assert storage.save_and_enqueue(edited) is True
    with storage.get_conn() as db:
        row = db.execute(
            "SELECT text, caption, urls, content_hash, updated_at FROM messages WHERE chat_id='4429917555' AND message_id=28157"
        ).fetchone()
        outbox = db.execute("SELECT status, payload FROM resource_outbox WHERE message_id=28157").fetchone()

    assert row["text"] == ""
    assert row["caption"] == "测试剧 S02E01-S02E02"
    assert json.loads(row["urls"]) == ["https://pan.guangyapan.com/s/updated"]
    assert row["content_hash"] != first["content_hash"]
    assert row["updated_at"] != first["updated_at"]
    assert outbox["status"] == "PENDING"
    source = TelegramSourceMessage.model_validate(json.loads(outbox["payload"]))
    assert source.caption == "测试剧 S02E01-S02E02"
    assert source.metadata["resource_content_hash"] == row["content_hash"]


def test_resource_storage_adds_edit_columns_without_losing_legacy_rows(tmp_path):
    path = tmp_path / "legacy_resource_messages.db"
    with sqlite3.connect(path) as db:
        db.executescript(
            """
            CREATE TABLE messages (
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
            CREATE TABLE resource_outbox (
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
            INSERT INTO messages(chat_id,chat_title,message_id,text,urls,source_type,date)
            VALUES('4429917555','光鸭云盘资源频道',28157,'旧标题','[]','telegram_channel','2026-09-23T00:00:00Z');
            """
        )

    ResourceStorage(str(path))
    with sqlite3.connect(path) as db:
        columns = {row[1] for row in db.execute("PRAGMA table_info(messages)")}
        row = db.execute("SELECT text, caption, message_id, content_hash, updated_at FROM messages").fetchone()

    assert {"caption", "content_hash", "updated_at"} <= columns
    assert (row[0], row[1], row[2]) == ("旧标题", "", 28157)
    assert row[3]
    assert row[4] == "2026-09-23T00:00:00Z"
