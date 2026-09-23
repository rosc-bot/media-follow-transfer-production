"""Comprehensive tests for the unified Telegram Gateway, Event Router, and Fault Isolation."""

import asyncio
from datetime import UTC, datetime

import pytest

from app.core.constants import (
    CHANNEL_ROLE_MANUAL_INGEST,
    CHANNEL_ROLE_RESOURCE,
    SOURCE_MANUAL_FORWARD,
    SOURCE_TELEGRAM_CHANNEL,
)
from app.models.channel import ChannelSetting
from app.monitor.event_router import EventRouter
from app.monitor.resource_pipeline import (
    ResourceStorage,
    deliver_outbox_pending,
    serialize_resource_message,
)
from app.monitor.summary_pipeline import SummaryStorage, summary_worker


class DummyChat:
    def __init__(self, chat_id: int, title: str = "Test Chat", broadcast: bool = False, username: str | None = None):
        self.id = chat_id
        self.title = title
        self.broadcast = broadcast
        self.username = username
        self.first_name = None


class DummyMessage:
    def __init__(self, msg_id: int, text: str, fwd: bool = False, entities: list | None = None, buttons: list | None = None):
        self.id = msg_id
        self.text = text
        self.message = text
        self.media = None
        self.date = datetime.now(UTC)
        self.sender_id = 12345
        self.sender = type("Sender", (), {"first_name": "Test", "last_name": "User", "username": "testuser"})()
        self.entities = entities or []
        self.buttons = buttons
        self.reply_to = None
        self.fwd_from = type("Fwd", (), {})() if fwd else None
        self.forward = self.fwd_from


class DummyEvent:
    def __init__(self, chat: DummyChat, msg: DummyMessage):
        self.chat = chat
        self.chat_id = chat.id
        self.message = msg

    async def get_chat(self):
        return self.chat


@pytest.mark.asyncio
async def test_event_router_demuxes_channels_and_groups():
    summary_q = asyncio.Queue()
    resource_q = asyncio.Queue()

    resource_channel_setting = ChannelSetting(
        channel_id="-1004429917555",
        channel_name="光鸭资源频道",
        role=CHANNEL_ROLE_RESOURCE,
        enabled=True,
    )
    resource_group_setting = ChannelSetting(
        channel_id="-1003702243011",
        channel_name="光鸭资源群",
        role=CHANNEL_ROLE_RESOURCE,
        enabled=True,
    )
    settings_map = {
        "-1004429917555": resource_channel_setting,
        "-1003702243011": resource_group_setting,
    }

    router = EventRouter(summary_q, resource_q, settings_map)

    # 1. Broadcast Resource Channel
    chan_chat = DummyChat(-1004429917555, "光鸭资源频道", broadcast=True)
    chan_msg = DummyMessage(101, "新片 https://pan.guangyapan.com/s/test1")
    await router.route_event(DummyEvent(chan_chat, chan_msg))

    assert resource_q.qsize() == 1
    assert summary_q.qsize() == 0  # Broadcast channels do not go to group summary

    # 2. Ordinary Chat Group (not in settings)
    ord_chat = DummyChat(-1001234567890, "日常吹水群", broadcast=False)
    ord_msg = DummyMessage(201, "今天有什么好玩的？")
    await router.route_event(DummyEvent(ord_chat, ord_msg))

    assert summary_q.qsize() == 1  # Captured for summary
    assert resource_q.qsize() == 1  # Resource queue untouched

    # 3. Resource Group (both a group and a configured resource channel)
    res_group_chat = DummyChat(-1003702243011, "光鸭资源分享群", broadcast=False)
    res_group_msg = DummyMessage(301, "分享一个剧集 https://pan.guangyapan.com/s/test2")
    await router.route_event(DummyEvent(res_group_chat, res_group_msg))

    assert summary_q.qsize() == 2  # Group captured for summary
    assert resource_q.qsize() == 2  # AND routed to resource ingestion!


@pytest.mark.asyncio
async def test_manual_forward_recognition():
    manual_setting = ChannelSetting(
        channel_id="-1003961136374",
        channel_name="测试频道",
        role=CHANNEL_ROLE_MANUAL_INGEST,
        enabled=True,
        accept_forward=True,
    )
    chat = DummyChat(-1003961136374, "测试频道", broadcast=True)

    # A. Forwarded message -> accepted as manual_forward
    fwd_msg = DummyMessage(501, "https://pan.guangyapan.com/s/fwd1", fwd=True)
    payload_fwd = serialize_resource_message(fwd_msg, chat, manual_setting)
    assert payload_fwd is not None
    assert payload_fwd["is_forward"] is True
    assert payload_fwd["source_type"] == SOURCE_MANUAL_FORWARD
    assert "https://pan.guangyapan.com/s/fwd1" in payload_fwd["urls"]

    # B. Non-forwarded message in test channel requiring forward -> ignored
    direct_msg = DummyMessage(502, "https://pan.guangyapan.com/s/direct", fwd=False)
    payload_direct = serialize_resource_message(direct_msg, chat, manual_setting)
    assert payload_direct is None


@pytest.mark.asyncio
async def test_summary_and_kb_isolation(tmp_path):
    db_file = str(tmp_path / "tg_messages.db")
    storage = SummaryStorage(db_file)

    summary_q = asyncio.Queue()
    kb_q = asyncio.Queue()
    stop_event = asyncio.Event()

    worker_task = asyncio.create_task(summary_worker(summary_q, storage, kb_queue=kb_q, stop_event=stop_event))

    # Send a message to summary queue
    msg = DummyMessage(1001, "Hello world in group")
    await summary_q.put({"chat_id": -1004495899387, "chat_title": "人🐔局（执着白嫖）", "message": msg, "kind": "group"})

    await asyncio.sleep(0.1)
    stop_event.set()
    await worker_task

    # Verify message was committed to tg_messages.db
    with storage.get_conn() as conn:
        row = conn.execute("SELECT chat_id, message_id, text FROM messages WHERE message_id=1001").fetchone()
        assert row is not None
        assert row["chat_id"] == -1004495899387
        assert row["text"] == "Hello world in group"

    # Verify message was enqueued to KB queue asynchronously
    assert kb_q.qsize() == 1


@pytest.mark.asyncio
async def test_resource_outbox_fault_isolation(tmp_path):
    db_file = str(tmp_path / "resource_messages.db")
    storage = ResourceStorage(db_file)

    # 1. Save and enqueue resource message
    payload = {
        "channel_id": "-1004429917555",
        "channel_title": "资源频道",
        "message_id": 888,
        "text": "影视资源 https://pan.guangyapan.com/s/resource888",
        "urls": ["https://pan.guangyapan.com/s/resource888"],
        "source_type": SOURCE_TELEGRAM_CHANNEL,
        "is_forward": False,
        "published_at": datetime.now(UTC).isoformat(),
    }
    assert storage.save_and_enqueue(payload) is True

    # 2. Simulate Media System Down (ingest_handler raises Exception)
    async def failing_ingest(item):
        raise ConnectionRefusedError("Media ingest system offline")

    delivered = await deliver_outbox_pending(storage, failing_ingest)
    assert delivered == 0

    # 3. Check outbox status: must be RETRY_WAIT with error message, NOT crashed!
    with storage.get_conn() as conn:
        row = conn.execute("SELECT status, attempt_count, last_error FROM resource_outbox WHERE message_id=888").fetchone()
        assert row is not None
        assert row["status"] == "RETRY_WAIT"
        assert row["attempt_count"] == 1
        assert "Media ingest system offline" in row["last_error"]

    # 4. Simulate Media System Recovered
    async def successful_ingest(item):
        return True

    delivered = await deliver_outbox_pending(storage, successful_ingest)
    assert delivered == 1

    # 5. Check outbox status: must be SENT
    with storage.get_conn() as conn:
        row = conn.execute("SELECT status, sent_at FROM resource_outbox WHERE message_id=888").fetchone()
        assert row["status"] == "SENT"
        assert row["sent_at"] is not None
