"""Phase 2B tests: resource whitelist, backfill idempotency, link extraction."""

import sqlite3

from app.core.constants import CHANNEL_ROLE_PUBLISH_ONLY
from app.ingest.resource_link_extractor import ResourceLinkExtractor
from app.monitor.source_allowlist import is_channel_scout_allowed
from tools.backfill_resource_messages import (
    backfill,
    ensure_schema,
    full_to_short,
    normalize_chat_id,
)


def _make_source_db(path, rows):
    with sqlite3.connect(path) as db:
        db.execute("""
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL, chat_title TEXT, message_id INTEGER,
                sender_id INTEGER, sender_name TEXT, text TEXT, date REAL,
                is_reply INTEGER DEFAULT 0, reply_to_msg_id INTEGER, urls TEXT,
                UNIQUE(chat_id, message_id)
            )
        """)
        for value in rows:
            db.execute(
                "INSERT INTO messages(chat_id, chat_title, message_id, sender_id, sender_name, text, date, urls) "
                "VALUES(?,?,?,?,?,?,?,?)",
                value,
            )
        db.commit()


def _count(db_path):
    with sqlite3.connect(db_path) as db:
        return db.execute("SELECT count(*) FROM messages").fetchone()[0]


class FakeSetting:
    def __init__(self, role, enabled=True):
        self.role = role
        self.enabled = enabled


# 2. 普通群不回填 / 3. PUBLISH_ONLY 不回填
def test_backfill_only_whitelisted_channels(tmp_path):
    src = tmp_path / "tg_messages.db"
    dst = tmp_path / "resource_messages.db"
    _make_source_db(src, [
        (-1003702243011, "资源群", 1, 123, "甲", "电影 S01E01 https://pan.guangyapan.com/s/abc", 1000.0, "[]"),
        (-1004431514859, "私密休息室", 2, 456, "乙", "闲聊内容没有链接", 1001.0, "[]"),
    ])
    ensure_schema(str(dst))
    report = backfill(source_db=str(src), target_db=str(dst), whitelist=["3702243011"], apply=True)
    assert report["mode"] == "apply"
    with sqlite3.connect(dst) as db:
        rows = db.execute("SELECT chat_id, message_id, sender_id, sender_name FROM messages").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "3702243011"
        assert rows[0][2] == 123  # sender preserved
        assert rows[0][3] == "甲"


# 4. 重复 chat_id+message_id 幂等
def test_backfill_idempotent_on_chat_message(tmp_path):
    src = tmp_path / "tg_messages.db"
    dst = tmp_path / "resource_messages.db"
    _make_source_db(src, [
        (-1003702243011, "资源群", 1, 1, "甲", "剧 S01E01 https://pan.guangyapan.com/s/abc", 1000.0, "[]"),
    ])
    ensure_schema(str(dst))
    first = backfill(source_db=str(src), target_db=str(dst), whitelist=["3702243011"], apply=True)
    second = backfill(source_db=str(src), target_db=str(dst), whitelist=["3702243011"], apply=True)
    assert first["channels"][0]["to_add"] == 1
    assert second["channels"][0]["to_add"] == 0
    assert second["channels"][0]["already_exists"] == 1
    assert _count(dst) == 1


# 1+5. 无分享链接的资源频道消息：统计但绝不入库
def test_backfill_skips_messages_without_supported_link(tmp_path):
    src = tmp_path / "tg_messages.db"
    dst = tmp_path / "resource_messages.db"
    _make_source_db(src, [
        (-1003702243011, "资源群", 1, 1, "甲", "欢迎新人，没有链接", 1000.0, "[]"),
        (-1003702243011, "资源群", 2, 1, "甲", "电影 S01E01 https://t.me/random/1", 1001.0, "[]"),
    ])
    ensure_schema(str(dst))
    report = backfill(source_db=str(src), target_db=str(dst), whitelist=["3702243011"], apply=True)
    assert _count(dst) == 0
    channel = report["channels"][0]
    assert channel["no_share_url"] == 2
    assert channel["to_add"] == 0


def test_normalize_chat_id():
    assert full_to_short("-1003702243011") == "3702243011"
    assert normalize_chat_id("3702243011") == "3702243011"


def test_is_channel_scout_allowed_roles():
    assert is_channel_scout_allowed(FakeSetting("RESOURCE")) is True
    assert is_channel_scout_allowed(FakeSetting("MANUAL_INGEST")) is True
    assert is_channel_scout_allowed(FakeSetting(CHANNEL_ROLE_PUBLISH_ONLY)) is False
    assert is_channel_scout_allowed(FakeSetting("RESOURCE", enabled=False)) is False
    assert is_channel_scout_allowed(None) is False


def test_resource_link_extractor_supported():
    extractor = ResourceLinkExtractor
    assert extractor.has_supported_share_url(["https://pan.guangyapan.com/s/abc"]) is True
    assert extractor.has_supported_share_url(["https://pan.quark.cn/s/abc"]) is True
    assert extractor.has_supported_share_url(["https://t.me/random/1"]) is False
    assert extractor.has_supported_share_url([]) is False
    assert extractor.first_supported_share_url(
        ["https://t.me/x", "https://pan.guangyapan.com/s/a"]
    ) == "https://pan.guangyapan.com/s/a"
