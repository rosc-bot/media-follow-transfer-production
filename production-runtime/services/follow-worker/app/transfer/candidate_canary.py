"""Read-only Canary feasibility diagnostics over resource_candidates."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.follow.episode_keys import canonical_episode_key
from app.models.cloud import CloudConfig, CloudDiskInventory
from app.models.resource import Resource
from app.models.resource_candidate import ResourceCandidate
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.transfer.canary_preflight import (
    ALLOWED_SOURCE_TYPES,
    CANARY_REJECTED,
    CANARY_SAFE,
    NEEDS_REVIEW,
    _episode_numbers,
    validate_remote_canary,
)
from app.transfer.status import SUCCESS_TERMINAL_STATUSES, TransferStatus
from app.transfer.task_identity import task_matches_episode


async def apply_deterministic_candidate_status(
    db: AsyncSession,
    *,
    candidate: ResourceCandidate,
    preflight_status: str,
    failure_code: str | None,
    failure_detail: str,
) -> bool:
    """Apply only an unambiguous read-only probe conclusion to one candidate.

    Phase 2E production listing is intentionally dry-run and does not call this
    function. If a later operator explicitly enables the status-update mode,
    temporary transport outcomes remain retryable and only deterministic content
    failures permanently retire a candidate. Queue tasks, resources, watchlists
    and collected fields are never touched here.
    """
    permanent = {
        "INVALID_SHARE": "INVALID_SHARE",
        "SHARE_NOT_FOUND": "INVALID_SHARE",
        "NO_VIDEO": "NO_VIDEO",
        "NO_VIDEO_FILES": "NO_VIDEO",
        "EPISODE_MISMATCH": "EPISODE_MISMATCH",
    }
    if preflight_status == CANARY_SAFE:
        next_status = "VALIDATED"
    elif failure_code in permanent:
        next_status = permanent[failure_code]
    elif failure_code in {"NETWORK_TIMEOUT", "RATE_LIMITED", "SHARE_API_ERROR", "NETWORK_ERROR", "REMOTE_5XX"}:
        next_status = "TEMPORARY_FAILED"
    elif failure_code in {"ACCOUNT_AUTH_FAILED", "ACCOUNT_AUTH_MISSING"}:
        next_status = "AUTH_BLOCKED"
    else:
        return False
    detail = failure_detail[:2000]
    changed = (
        candidate.status != next_status
        or candidate.failure_category != failure_code
        or candidate.failure_reason != detail
    )
    candidate.status = next_status
    candidate.failure_category = failure_code
    candidate.failure_reason = detail or None
    candidate.last_checked_at = datetime.now(UTC)
    await db.flush()
    return changed


def _candidate_sort_key(row: dict) -> tuple:
    return (
        0 if row["status"] == "VALIDATED" else 1,
        -row["discovered_at"].timestamp(),
        0 if row["source_type"] == "watchlist_scout" else 1,
        0 if row.get("explicit_full_key") else 1,
        int((row.get("remote_validation") or {}).get("share", {}).get("video_count") or 999999) > 1,
        row["candidate_id"],
    )


def _row(candidate: ResourceCandidate) -> dict:
    return {
        "candidate_id": candidate.id,
        "tmdb_id": candidate.tmdb_id,
        "title": candidate.title,
        "season": candidate.season,
        "episode_key": candidate.episode_key,
        "status": str(candidate.status),
        "source_type": candidate.source_type or "",
        "discovered_at": candidate.discovered_at,
        "preflight_status": CANARY_SAFE,
        "failure_code": None,
        "failure_detail": "",
        "recommended_for_canary": False,
        "explicit_full_key": False,
        "remote_probe": "NOT_RUN",
    }


def _set_result(row: dict, status: str, code: str | None, detail: str) -> None:
    row["preflight_status"] = status
    row["failure_code"] = code
    row["failure_detail"] = detail
    row["recommended_for_canary"] = status == CANARY_SAFE


async def _static_candidate_check(
    db: AsyncSession,
    candidate: ResourceCandidate,
    cfg: CloudConfig | None,
    tasks: list[TransferQueueTask],
) -> tuple[dict, bool]:
    """Run DB-only candidate checks and say whether a remote probe is needed."""
    row = _row(candidate)
    watch = await db.scalar(
        select(SeriesWatchlist).where(
            SeriesWatchlist.tmdb_id == candidate.tmdb_id,
            SeriesWatchlist.season == candidate.season,
            SeriesWatchlist.title == candidate.title,
            SeriesWatchlist.status == "FOLLOWING",
        )
    )
    if watch is None:
        _set_result(row, CANARY_REJECTED, "WATCHLIST_NOT_FOLLOWING", "no exact active watchlist")
        return row, False
    if candidate.episode_key in {str(value) for value in (watch.collected_episodes or [])}:
        _set_result(row, CANARY_REJECTED, "ALREADY_COLLECTED", "watchlist already collected")
        return row, False
    if candidate.source_type not in ALLOWED_SOURCE_TYPES:
        _set_result(row, CANARY_REJECTED, "SOURCE_TYPE_UNSUPPORTED", str(candidate.source_type))
        return row, False
    if candidate.queue_task_id is not None:
        _set_result(row, NEEDS_REVIEW, "ALREADY_QUEUED", f"queue_task_id={candidate.queue_task_id}")
        return row, False

    inventory = (
        await db.execute(
            select(CloudDiskInventory).where(
                CloudDiskInventory.tmdb_id == candidate.tmdb_id,
                CloudDiskInventory.season == candidate.season,
            )
        )
    ).scalars().all()
    if _episode_numbers([candidate.episode_key]) & {int(item.episode) for item in inventory}:
        _set_result(row, CANARY_REJECTED, "ALREADY_IN_CLOUD", "inventory episode exists")
        return row, False

    target = {
        key
        for value in [candidate.episode_key]
        if (key := canonical_episode_key(candidate.season, value)) is not None
    }
    resource_ids = {item.resource_id for item in tasks if item.resource_id is not None}
    resources = {
        item.id: item
        for item in (await db.scalars(select(Resource).where(Resource.id.in_(resource_ids)))).all()
    } if resource_ids else {}
    same_episode = [
        item
        for item in tasks
        if task_matches_episode(
            item.payload,
            tmdb_id=candidate.tmdb_id,
            season=candidate.season,
            episode_keys=target,
            resource=resources.get(item.resource_id),
        )
    ]
    completed = [item.id for item in same_episode if str(item.status) in {str(value) for value in SUCCESS_TERMINAL_STATUSES}]
    if completed:
        _set_result(row, CANARY_REJECTED, "DUPLICATE_SUCCESS", f"task_ids={completed}")
        return row, False
    active = [
        item.id
        for item in same_episode
        if item.status in {TransferStatus.QUEUED, TransferStatus.RETRY_WAIT, TransferStatus.RUNNING}
    ]
    if active:
        _set_result(row, NEEDS_REVIEW, "DUPLICATE_ACTIVE", f"task_ids={active}")
        return row, False

    if cfg is None or not cfg.enabled or not cfg.auth_ref or not (cfg.target_folder_id or cfg.ongoing_target_folder_id):
        _set_result(row, NEEDS_REVIEW, "DESTINATION_INVALID", "cloud config incomplete")
        return row, False
    return row, True


async def _run_remote_probe(
    semaphore: asyncio.Semaphore,
    candidate: ResourceCandidate,
    cfg: CloudConfig,
    remote_validator: Callable[..., Awaitable[dict]],
) -> dict:
    async with semaphore:
        try:
            return await remote_validator(
                provider=candidate.provider,
                share_url=candidate.share_url,
                auth_ref=cfg.auth_ref,
                target_folder_id=cfg.target_folder_id or cfg.ongoing_target_folder_id,
                episode_keys=[candidate.episode_key],
                season=candidate.season,
            )
        except TimeoutError as exc:
            return {
                "share_accessible": False,
                "failure_code": "NETWORK_TIMEOUT",
                "failure_detail": f"{type(exc).__name__}: {str(exc)[:240]}",
            }
        except Exception as exc:  # noqa: BLE001 - probe must fail closed without aborting siblings
            return {
                "share_accessible": False,
                "failure_code": "SHARE_API_ERROR",
                "failure_detail": f"{type(exc).__name__}: {str(exc)[:240]}",
            }


def _apply_remote_result(row: dict, remote: dict) -> None:
    row["remote_validation"] = remote
    row["remote_probe"] = "COMPLETED"
    row["explicit_full_key"] = any(
        item.get("match_result") == "MATCH" and item.get("detected_season") is not None
        for item in remote.get("episode_diagnostics", [])
    )
    if not remote.get("share_accessible", remote.get("share_readable", False)):
        code = remote.get("failure_code") or "SHARE_UNREADABLE"
        transient = code in {
            "NETWORK_TIMEOUT",
            "RATE_LIMITED",
            "SHARE_API_ERROR",
            "NETWORK_ERROR",
            "REMOTE_5XX",
            "ACCOUNT_AUTH_FAILED",
            "ACCOUNT_AUTH_MISSING",
        }
        _set_result(
            row,
            NEEDS_REVIEW if transient else CANARY_REJECTED,
            code,
            remote.get("failure_detail") or "share inaccessible",
        )
    elif not remote.get("has_video"):
        _set_result(row, CANARY_REJECTED, "NO_VIDEO", "share read succeeded but no supported video")
    elif not remote.get("episode_match"):
        _set_result(row, CANARY_REJECTED, "EPISODE_MISMATCH", "no matching regular episode")
    elif not remote.get("destination_auth") or not remote.get("destination_read"):
        _set_result(
            row,
            NEEDS_REVIEW,
            remote.get("failure_code") or "ACCOUNT_AUTH_FAILED",
            remote.get("failure_detail") or "destination cannot be read",
        )
    else:
        _set_result(row, CANARY_SAFE, None, "all read-only resource checks passed")


async def list_canary_resource_candidates(
    *,
    session_factory=AsyncSessionLocal,
    remote_validator: Callable[..., Awaitable[dict]] = validate_remote_canary,
    hours: int = 24,
    limit: int = 50,
    concurrency: int = 4,
) -> dict:
    """Evaluate a bounded recent candidate sample without queue/task writes.

    ``candidate_total_24h`` reports the complete recent ledger population;
    ``total_checked`` is the bounded sample actually probed. Network probes are
    concurrent but capped, and the SQLAlchemy session is never shared by probes.
    """
    cutoff = datetime.now(UTC) - timedelta(hours=max(1, hours))
    async with session_factory() as db:
        total_recent = await db.scalar(
            select(func.count()).select_from(ResourceCandidate).where(ResourceCandidate.discovered_at >= cutoff)
        )
        live_recent = await db.scalar(
            select(func.count())
            .select_from(ResourceCandidate)
            .where(ResourceCandidate.discovered_at >= cutoff)
            .where(ResourceCandidate.status.in_(["DISCOVERED", "VALIDATED"]))
        )
        candidates = (
            await db.execute(
                select(ResourceCandidate)
                .where(ResourceCandidate.discovered_at >= cutoff)
                .where(ResourceCandidate.status.in_(["DISCOVERED", "VALIDATED"]))
                .order_by(ResourceCandidate.discovered_at.desc())
                .limit(max(1, limit))
            )
        ).scalars().all()
        cfg = await db.scalar(select(CloudConfig).where(CloudConfig.name == "guangya"))
        tasks = list((await db.execute(select(TransferQueueTask))).scalars().all())

        rows: list[dict] = []
        pending: list[tuple[dict, ResourceCandidate]] = []
        for candidate in candidates:
            item, needs_probe = await _static_candidate_check(db, candidate, cfg, tasks)
            rows.append(item)
            if needs_probe and cfg is not None:
                pending.append((item, candidate))

        semaphore = asyncio.Semaphore(max(1, concurrency))
        if cfg is not None:
            probe_results = await asyncio.gather(
                *(_run_remote_probe(semaphore, candidate, cfg, remote_validator) for _row_item, candidate in pending)
            )
        else:
            probe_results = []
        for (item, _candidate), remote in zip(pending, probe_results, strict=True):
            _apply_remote_result(item, remote)

    rows.sort(key=_candidate_sort_key)
    counts = Counter(row["preflight_status"] for row in rows)
    return {
        "candidate_total_24h": int(total_recent or 0),
        "candidate_live_24h": int(live_recent or 0),
        "total_checked": len(rows),
        "probe_completed": sum(row["remote_probe"] == "COMPLETED" for row in rows),
        "CANARY_RESOURCE_SAFE": counts[CANARY_SAFE],
        "CANARY_RESOURCE_REJECTED": counts[CANARY_REJECTED],
        "NEEDS_REVIEW": counts[NEEDS_REVIEW],
        "recommended_candidate_ids": [
            row["candidate_id"] for row in rows if row["recommended_for_canary"]
        ][:10],
        "rows": rows,
    }
