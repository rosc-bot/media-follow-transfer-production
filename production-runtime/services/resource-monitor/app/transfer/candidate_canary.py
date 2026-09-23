"""Read-only Canary feasibility diagnostics over resource_candidates."""

from __future__ import annotations

from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.models.cloud import CloudConfig, CloudDiskInventory
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
from app.transfer.status import TransferStatus


async def apply_deterministic_candidate_status(
    db,
    *,
    candidate: ResourceCandidate,
    preflight_status: str,
    failure_code: str | None,
    failure_detail: str,
) -> bool:
    """Apply only an unambiguous read-only probe conclusion to the candidate ledger.

    This function never reads or mutates queue tasks, resources, or watchlists.
    Temporary transport outcomes are retryable; only explicit permanent share/
    content facts permanently retire a candidate.
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
    changed = candidate.status != next_status or candidate.failure_category != failure_code or candidate.failure_reason != failure_detail[:2000]
    candidate.status = next_status
    candidate.failure_category = failure_code
    candidate.failure_reason = failure_detail[:2000] or None
    candidate.last_checked_at = datetime.now(UTC)
    await db.flush()
    return changed


def _candidate_sort_key(row: dict) -> tuple:
    return (
        0 if row["status"] == "VALIDATED" else 1,
        -row["discovered_at"].timestamp(),
        0 if row["source_type"] == "watchlist_scout" else 1,
        0 if row.get("explicit_full_key") else 1,
        row["candidate_id"],
    )


async def list_canary_resource_candidates(
    *,
    session_factory=AsyncSessionLocal,
    remote_validator: Callable[..., Awaitable[dict]] = validate_remote_canary,
    hours: int = 24,
    limit: int = 500,
) -> dict:
    """Evaluate candidates without queue/task/resource/collected writes."""
    cutoff = datetime.now(UTC) - timedelta(hours=max(1, hours))
    rows: list[dict] = []
    async with session_factory() as db:
        candidates = (await db.execute(
            select(ResourceCandidate)
            .where(ResourceCandidate.discovered_at >= cutoff)
            .where(ResourceCandidate.status.in_(["DISCOVERED", "VALIDATED"]))
            .order_by(ResourceCandidate.discovered_at.desc())
            .limit(limit)
        )).scalars().all()
        cfg = await db.scalar(select(CloudConfig).where(CloudConfig.name == "guangya"))
        for candidate in candidates:
            row = {
                "candidate_id": candidate.id,
                "tmdb_id": candidate.tmdb_id,
                "title": candidate.title,
                "season": candidate.season,
                "episode_key": candidate.episode_key,
                "status": candidate.status,
                "source_type": candidate.source_type or "",
                "discovered_at": candidate.discovered_at,
                "preflight_status": CANARY_SAFE,
                "failure_code": None,
                "failure_detail": "",
                "recommended_for_canary": False,
                "explicit_full_key": False,
            }
            watch = await db.scalar(select(SeriesWatchlist).where(
                SeriesWatchlist.tmdb_id == candidate.tmdb_id,
                SeriesWatchlist.season == candidate.season,
                SeriesWatchlist.title == candidate.title,
                SeriesWatchlist.status == "FOLLOWING",
            ))
            if watch is None:
                row.update(preflight_status=CANARY_REJECTED, failure_code="WATCHLIST_NOT_FOLLOWING", failure_detail="no exact active watchlist")
            elif candidate.episode_key in {str(value) for value in (watch.collected_episodes or [])}:
                row.update(preflight_status=CANARY_REJECTED, failure_code="ALREADY_COLLECTED", failure_detail="watchlist already collected")
            elif candidate.source_type not in ALLOWED_SOURCE_TYPES:
                row.update(preflight_status=CANARY_REJECTED, failure_code="SOURCE_TYPE_UNSUPPORTED", failure_detail=str(candidate.source_type))
            else:
                inv = (await db.execute(select(CloudDiskInventory).where(
                    CloudDiskInventory.tmdb_id == candidate.tmdb_id,
                    CloudDiskInventory.season == candidate.season,
                ))).scalars().all()
                if _episode_numbers([candidate.episode_key]) & {int(item.episode) for item in inv}:
                    row.update(preflight_status=CANARY_REJECTED, failure_code="ALREADY_IN_CLOUD", failure_detail="inventory episode exists")
                else:
                    tasks = (await db.execute(select(TransferQueueTask))).scalars().all()
                    target = {candidate.episode_key}
                    completed = [
                        item.id for item in tasks
                        if item.status == TransferStatus.COMPLETED
                        and int((item.payload or {}).get("tmdb_id") or -1) == candidate.tmdb_id
                        and target & {str(value) for value in ((item.payload or {}).get("episode_keys") or [])}
                    ]
                    if completed:
                        row.update(preflight_status=CANARY_REJECTED, failure_code="DUPLICATE_SUCCESS", failure_detail=f"task_ids={completed}")
                    elif cfg is None or not cfg.enabled or not cfg.auth_ref or not (cfg.target_folder_id or cfg.ongoing_target_folder_id):
                        row.update(preflight_status=NEEDS_REVIEW, failure_code="DESTINATION_INVALID", failure_detail="cloud config incomplete")
                    else:
                        remote = await remote_validator(
                            provider=candidate.provider,
                            share_url=candidate.share_url,
                            auth_ref=cfg.auth_ref,
                            target_folder_id=cfg.target_folder_id or cfg.ongoing_target_folder_id,
                            episode_keys=[candidate.episode_key],
                            season=candidate.season,
                        )
                        row["remote_validation"] = remote
                        row["explicit_full_key"] = any(
                            item.get("match_result") == "MATCH" and item.get("detected_season") is not None
                            for item in remote.get("episode_diagnostics", [])
                        )
                        if not remote.get("share_accessible", remote.get("share_readable", False)):
                            row.update(preflight_status=CANARY_REJECTED, failure_code=remote.get("failure_code") or "SHARE_UNREADABLE", failure_detail=remote.get("failure_detail") or "share inaccessible")
                        elif not remote.get("has_video"):
                            row.update(preflight_status=CANARY_REJECTED, failure_code="NO_VIDEO", failure_detail="no video")
                        elif not remote.get("episode_match"):
                            row.update(preflight_status=CANARY_REJECTED, failure_code="EPISODE_MISMATCH", failure_detail="no matching episode")
                        elif not remote.get("destination_auth") or not remote.get("destination_read"):
                            row.update(preflight_status=CANARY_REJECTED, failure_code=remote.get("failure_code") or "ACCOUNT_AUTH_FAILED", failure_detail=remote.get("failure_detail") or "destination cannot be read")
            row["recommended_for_canary"] = row["preflight_status"] == CANARY_SAFE
            rows.append(row)
    rows.sort(key=_candidate_sort_key)
    counts = Counter(row["preflight_status"] for row in rows)
    return {
        "total_checked": len(rows),
        "CANARY_RESOURCE_SAFE": counts[CANARY_SAFE],
        "CANARY_RESOURCE_REJECTED": counts[CANARY_REJECTED],
        "NEEDS_REVIEW": counts[NEEDS_REVIEW],
        "recommended_candidate_ids": [row["candidate_id"] for row in rows if row["recommended_for_canary"]][:10],
        "rows": rows,
    }
