"""Bounded Guangya network retry classification and provider circuit breaker."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bot_settings import BotSettings

_PROVIDER_HEALTH_KEY = "transfer_provider_health_guangya"
_WINDOW = timedelta(minutes=5)
_COOLDOWN = timedelta(minutes=3)
_BREAKER_THRESHOLD = 3
_PROBE_LEASE = timedelta(seconds=60)
_BACKOFF_SECONDS = (60, 180, 300, 600, 900)
_NETWORK_MARKERS = (
    "CONNECTTIMEOUT", "READTIMEOUT", "WRITETIMEOUT", "POOLTIMEOUT", "TIMEOUTERROR",
    "CONNECTERROR", "NETWORKERROR", "NETWORK_TIMEOUT", "NETWORK_ERROR", "REMOTE_5XX",
    "HTTPSTATUSERROR:429", "HTTPSTATUSERROR:500", "HTTPSTATUSERROR:501",
    "HTTPSTATUSERROR:502", "HTTPSTATUSERROR:503", "HTTPSTATUSERROR:504",
    "RATE_LIMITED", "RATE LIMIT",
)


def is_retryable_provider_failure(reason: str, *, detail: Any = "", stage: str = "") -> bool:
    """Return true only for transient provider transport/service failures.

    A preflight wall timeout counts as a network failure only while executing a
    remote stage. Pagination, identity, and ledger conflicts remain review-only.
    """
    code = str(reason or "").strip().upper()
    current_stage = str(stage or "").strip().upper()
    text = f"{code} {detail}".upper()
    if code in {"NETWORK_TIMEOUT", "NETWORK_ERROR", "REMOTE_5XX", "RATE_LIMITED", "SHARE_READ_NETWORK_TIMEOUT"}:
        return True
    if code == "BATCH_PREFLIGHT_TIMEOUT":
        return current_stage in {"SHARE_PROBE", "CLOUD_PRESENCE_SCAN", "DESTINATION_LOOKUP"}
    if code in {"PHYSICAL_CLOUD_SCAN_TIMEOUT_UNVERIFIED", "DESTINATION_LOOKUP_TIMEOUT"}:
        return True
    if code in {"PHYSICAL_CLOUD_SCAN_API_ERROR", "DESTINATION_LOOKUP_API_ERROR", "BATCH_PREFLIGHT_EXCEPTION", "RUNTIME_PREFLIGHT_EXCEPTION", "TRANSIENT_PREFLIGHT_FAILURE"}:
        if "PAGINATION" in text or "IDENTITY" in text or "CONFLICT" in text:
            return False
        return any(marker in text for marker in _NETWORK_MARKERS) or bool(
            re.search(r"HTTPSTATUSERROR:(?:429|5\d{2})", text)
        )
    return (
        any(marker in text for marker in _NETWORK_MARKERS)
        or bool(re.search(r"HTTPSTATUSERROR:(?:429|5\d{2})", text))
    ) and current_stage in {
        "SHARE_PROBE", "CLOUD_PRESENCE_SCAN", "DESTINATION_LOOKUP"
    }


def provider_backoff_seconds(retry_count: int) -> int:
    """Bound task-level scheduling delay; network I/O retries remain short."""
    index = max(0, int(retry_count) - 1)
    return _BACKOFF_SECONDS[min(index, len(_BACKOFF_SECONDS) - 1)]


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(UTC)
    return current.replace(tzinfo=UTC) if current.tzinfo is None else current.astimezone(UTC)


def _load(raw: str | None) -> dict[str, Any]:
    try:
        value = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


async def _row_for_update(db: AsyncSession) -> BotSettings | None:
    return await db.scalar(
        select(BotSettings).where(BotSettings.key == _PROVIDER_HEALTH_KEY).with_for_update()
    )


async def _save(db: AsyncSession, row: BotSettings | None, value: dict[str, Any]) -> None:
    rendered = json.dumps(value, separators=(",", ":"), sort_keys=True)
    if row is None:
        db.add(BotSettings(key=_PROVIDER_HEALTH_KEY, val=rendered))
    else:
        row.val = rendered
    await db.flush()


async def record_provider_network_failure(
    db: AsyncSession,
    *,
    task_id: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Count distinct failing tasks in a rolling five-minute window."""
    current = _now(now)
    row = await _row_for_update(db)
    state = _load(row.val if row else None)
    cutoff = current - _WINDOW
    events: dict[str, str] = {}
    for event in state.get("failures") or []:
        if not isinstance(event, dict):
            continue
        try:
            occurred = _now(datetime.fromisoformat(str(event.get("at") or "")))
        except (TypeError, ValueError):
            continue
        if occurred >= cutoff:
            events[str(event.get("task_id"))] = occurred.isoformat()
    events.setdefault(str(int(task_id)), current.isoformat())
    degraded_until = current + _COOLDOWN if len(events) >= _BREAKER_THRESHOLD else None
    previous_until = state.get("cooldown_until")
    if previous_until:
        try:
            previous_dt = _now(datetime.fromisoformat(str(previous_until)))
            if previous_dt > current:
                degraded_until = max(degraded_until or current, previous_dt)
        except (TypeError, ValueError):
            pass
    state.update({
        "state": "PROVIDER_DEGRADED" if degraded_until else "CLOSED",
        "failures": [{"task_id": int(key), "at": value} for key, value in sorted(events.items())],
        "cooldown_until": degraded_until.isoformat() if degraded_until else None,
        "probe_started_at": None,
        "last_failure_at": current.isoformat(),
    })
    await _save(db, row, state)
    return {"state": state["state"], "distinct_tasks": len(events), "cooldown_until": state["cooldown_until"]}


async def provider_claim_gate(db: AsyncSession, *, now: datetime | None = None) -> str:
    """Return ALLOW, WAIT, or PROBE; a probe lease prevents duplicate health calls."""
    current = _now(now)
    row = await _row_for_update(db)
    state = _load(row.val if row else None)
    if state.get("state") != "PROVIDER_DEGRADED":
        return "ALLOW"
    try:
        cooldown_until = _now(datetime.fromisoformat(str(state.get("cooldown_until") or "")))
    except (TypeError, ValueError):
        cooldown_until = current + _COOLDOWN
        state["cooldown_until"] = cooldown_until.isoformat()
        await _save(db, row, state)
        return "WAIT"
    if current < cooldown_until:
        return "WAIT"
    probe_started = state.get("probe_started_at")
    if probe_started:
        try:
            if current - _now(datetime.fromisoformat(str(probe_started))) < _PROBE_LEASE:
                return "WAIT"
        except (TypeError, ValueError):
            pass
    state["probe_started_at"] = current.isoformat()
    await _save(db, row, state)
    return "PROBE"


async def complete_provider_health_probe(
    db: AsyncSession,
    *,
    success: bool,
    now: datetime | None = None,
) -> None:
    """Close after a successful directory-read probe; otherwise cool down again."""
    current = _now(now)
    row = await _row_for_update(db)
    state = _load(row.val if row else None)
    if success:
        state.update({
            "state": "CLOSED", "failures": [], "cooldown_until": None,
            "probe_started_at": None, "last_probe_at": current.isoformat(), "last_probe_success": True,
        })
    else:
        state.update({
            "state": "PROVIDER_DEGRADED", "cooldown_until": (current + _COOLDOWN).isoformat(),
            "probe_started_at": None, "last_probe_at": current.isoformat(), "last_probe_success": False,
        })
    await _save(db, row, state)


__all__ = [
    "complete_provider_health_probe", "is_retryable_provider_failure",
    "provider_backoff_seconds", "provider_claim_gate", "record_provider_network_failure",
]
