import json
import sqlite3

from app.scout.message_search import MessageSearch


def test_message_search_finds_title_candidate_older_than_global_200(tmp_path):
    db_path = tmp_path / "resource_messages.db"
    with sqlite3.connect(db_path) as db:
        db.execute(
            """CREATE TABLE messages (
                chat_id TEXT, chat_title TEXT, message_id INTEGER, text TEXT,
                urls TEXT, date TEXT
            )"""
        )
        db.executemany(
            "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("resource", "资源频道", message_id, f"无关资源 {message_id}", "[]", f"2026-09-24T00:{message_id % 60:02d}:00Z")
                for message_id in range(1, 201)
            ]
            + [
                ("resource", "资源频道", 201, "测试剧 S01E02 https://pan.guangyapan.com/s/example", json.dumps(["https://pan.guangyapan.com/s/example"]), "2026-01-01T00:00:00Z")
            ],
        )

    matches = MessageSearch(str(db_path)).search("测试剧", "S01E02")

    assert [message.message_id for message in matches] == [201]


def test_message_search_includes_caption_and_edit_watermark(tmp_path):
    db_path = tmp_path / "resource_messages.db"
    with sqlite3.connect(db_path) as db:
        db.execute(
            """CREATE TABLE messages (
                chat_id TEXT, chat_title TEXT, message_id INTEGER, text TEXT,
                caption TEXT, urls TEXT, date TEXT, content_hash TEXT, updated_at TEXT
            )"""
        )
        db.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("resource", "资源频道", 99, "", "剧名 S02E03", "[]", "2026-01-01", "hash-v2", "2026-09-23T16:00:00Z"),
        )

    matches = MessageSearch(str(db_path)).search("剧名", "S02E03")

    assert len(matches) == 1
    assert matches[0].text == "剧名 S02E03"
    assert matches[0].content_hash == "hash-v2"
    assert matches[0].updated_at == "2026-09-23T16:00:00Z"
