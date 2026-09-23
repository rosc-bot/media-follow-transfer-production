"""Transfer Worker heartbeat stored in the existing settings table."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.follow.bot_settings_service import BotSettingsService

TRANSFER_WORKER_HEARTBEAT_KEY = "transfer_worker_heartbeat"
TRANSFER_WORKER_HEARTBEAT_INTERVAL_SECONDS = 30
TRANSFER_WORKER_HEARTBEAT_STALE_SECONDS = 90


def _parse_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return None


def read_transfer_worker_heartbeat(
    raw: str | None,
    *,
    now: datetime | None = None,
    stale_after_seconds: int = TRANSFER_WORKER_HEARTBEAT_STALE_SECONDS,
) -> dict[str, Any]:
    """Return ONLINE data only while the heartbeat is fresh; stale means UNKNOWN."""
    unknown = {
        "status": "UNKNOWN",
        "worker_alive": None,
        "cloud_write_enabled": None,
        "release_commit": None,
        "updated_at": None,
    }
    try:
        payload = json.loads(raw or "")
        if not isinstance(payload, dict):
            return unknown
        timestamp = datetime.fromisoformat(str(payload.get("updated_at") or ""))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            current = current.replace(tzinfo=UTC)
        age = (current - timestamp).total_seconds()
        alive = payload.get("worker_alive") is True
        cloud_write = _parse_bool(payload.get("cloud_write_enabled"))
        release = str(payload.get("release_commit") or "").strip()
        if age < 0 or age > max(1, int(stale_after_seconds)) or not alive or cloud_write is None or not release:
            return unknown
        return {
            "status": "ONLINE",
            "worker_alive": True,
            "cloud_write_enabled": cloud_write,
            "release_commit": release,
            "updated_at": timestamp.astimezone(UTC).isoformat(),
        }
    except (TypeError, ValueError, json.JSONDecodeError):
        return unknown


def transfer_worker_settings_lines(state: dict[str, Any]) -> dict[str, str]:
    """Render the worker-owned cloud gate; stale state is never shown as disabled."""
    if str(state.get("status") or "").upper() != "ONLINE":
        return {"worker": "未知", "cloud_write": "未知", "release": "未知"}
    cloud_write = state.get("cloud_write_enabled")
    if not isinstance(cloud_write, bool) or not state.get("release_commit"):
        return {"worker": "未知", "cloud_write": "未知", "release": "未知"}
    return {
        "worker": "在线",
        "cloud_write": "启用" if cloud_write else "禁用",
        "release": str(state["release_commit"]),
    }


async def write_transfer_worker_heartbeat(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    cloud_write_enabled: bool,
    release_commit: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Persist one heartbeat without changing transfer pause or queue state."""
    timestamp = now or datetime.now(UTC)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    payload = {
        "worker_alive": True,
        "release_commit": str(release_commit or "unknown").strip() or "unknown",
        "cloud_write_enabled": bool(cloud_write_enabled),
        "updated_at": timestamp.astimezone(UTC).isoformat(),
    }
    async with session_factory() as db, db.begin():
        await BotSettingsService.set(
            db,
            TRANSFER_WORKER_HEARTBEAT_KEY,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )
    return payload
