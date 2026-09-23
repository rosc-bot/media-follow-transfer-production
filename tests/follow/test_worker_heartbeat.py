import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.follow.worker_heartbeat import (
    TRANSFER_WORKER_HEARTBEAT_KEY,
    read_transfer_worker_heartbeat,
    transfer_worker_settings_lines,
    write_transfer_worker_heartbeat,
)
from app.models.bot_settings import BotSettings
from app.workers import transfer_worker


def test_stale_or_malformed_worker_heartbeat_is_unknown_not_disabled():
    now = datetime(2026, 9, 24, 0, 0, tzinfo=UTC)
    stale = json.dumps({
        "worker_alive": True,
        "release_commit": "abc123",
        "cloud_write_enabled": False,
        "updated_at": (now - timedelta(seconds=91)).isoformat(),
    })

    state = read_transfer_worker_heartbeat(stale, now=now)
    malformed = read_transfer_worker_heartbeat("not-json", now=now)

    assert state["status"] == "UNKNOWN"
    assert state["cloud_write_enabled"] is None
    assert malformed["status"] == "UNKNOWN"
    assert malformed["cloud_write_enabled"] is None
    assert transfer_worker_settings_lines(state)["cloud_write"] == "未知"
    assert transfer_worker_settings_lines(malformed)["cloud_write"] == "未知"


def test_fresh_worker_heartbeat_reports_worker_gate_and_release():
    now = datetime(2026, 9, 24, 0, 0, tzinfo=UTC)
    raw = json.dumps({
        "worker_alive": True,
        "release_commit": "c9304a0a655cd9d5d3c294e74f3fbe089e35bb1e",
        "cloud_write_enabled": True,
        "updated_at": now.isoformat(),
    })

    state = read_transfer_worker_heartbeat(raw, now=now)

    assert state["status"] == "ONLINE"
    assert state["worker_alive"] is True
    assert state["cloud_write_enabled"] is True
    assert state["release_commit"] == "c9304a0a655cd9d5d3c294e74f3fbe089e35bb1e"
    assert transfer_worker_settings_lines(state) == {
        "worker": "在线",
        "cloud_write": "启用",
        "release": "c9304a0a655cd9d5d3c294e74f3fbe089e35bb1e",
    }


@pytest.mark.asyncio
async def test_transfer_worker_heartbeat_is_persisted_to_settings_store(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/heartbeat.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    await write_transfer_worker_heartbeat(
        sessions,
        cloud_write_enabled=True,
        release_commit="c9304a0a655cd9d5d3c294e74f3fbe089e35bb1e",
        now=datetime(2026, 9, 24, 0, 0, tzinfo=UTC),
    )

    async with sessions() as db:
        row = await db.scalar(select(BotSettings).where(BotSettings.key == TRANSFER_WORKER_HEARTBEAT_KEY))
    assert row is not None
    state = json.loads(row.val)
    assert state["worker_alive"] is True
    assert state["cloud_write_enabled"] is True
    assert state["release_commit"] == "c9304a0a655cd9d5d3c294e74f3fbe089e35bb1e"
    await engine.dispose()


@pytest.mark.asyncio
async def test_transfer_worker_runtime_heartbeat_starts_immediately_and_uses_worker_gate(monkeypatch):
    stop = asyncio.Event()
    writes = []
    monkeypatch.setattr(transfer_worker, "get_settings", lambda: SimpleNamespace(cloud_write_enabled=True))
    monkeypatch.setattr(transfer_worker, "_current_release_commit", lambda: "runtime-release")

    async def fake_write(session_factory, *, cloud_write_enabled, release_commit):
        writes.append((cloud_write_enabled, release_commit))
        stop.set()
        return {}

    monkeypatch.setattr(transfer_worker, "write_transfer_worker_heartbeat", fake_write)

    await transfer_worker._transfer_worker_heartbeat_loop(stop)

    assert writes == [(True, "runtime-release")]
