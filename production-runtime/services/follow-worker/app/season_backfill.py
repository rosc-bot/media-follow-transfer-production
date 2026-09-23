"""Read-only audit representation and narrow safe season backfill service."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.season_inference import infer_season


def build_audit_record(
    *,
    task_id: int | None,
    resource_id: int | None,
    tmdb_id: int | None,
    title: str | None,
    episode_key: str | None,
    payload_season: int | str | None,
    resource_season: int | None,
    watchlist_seasons: set[int] | list[int] | tuple[int, ...] | None,
    candidate_seasons: set[int] | list[int] | tuple[int, ...] | None,
    source_type: str | None = None,
) -> dict[str, Any]:
    """Create a JSON-safe audit row with strictly-derived season evidence."""
    inference = infer_season(
        episode_key=episode_key,
        watchlist_seasons=watchlist_seasons,
        candidate_seasons=candidate_seasons,
        resource_seasons={int(resource_season)} if resource_season is not None else set(),
        payload_seasons={payload_season} if payload_season is not None else set(),
    )
    return {
        "task_id": task_id,
        "resource_id": resource_id,
        "tmdb_id": tmdb_id,
        "title": title,
        "episode_key": episode_key,
        "payload_season": payload_season,
        "resource_season": resource_season,
        "watchlist_matches": sorted({int(value) for value in (watchlist_seasons or [])}),
        "candidate_matches": sorted({int(value) for value in (candidate_seasons or [])}),
        "inferred_season": inference.inferred_season,
        "evidence": inference.evidence,
        "confidence": inference.confidence,
        "conflict": list(inference.conflict.values) if inference.conflict else None,
        "source_type": source_type,
    }


async def apply_safe_backfill(db: AsyncSession, records: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Fill only absent season values for SAFE_INFER audit rows.

    No task status, attempts, identity, URLs, timestamps, or non-empty season
    values are modified.  The caller owns the outer transaction and must read
    back exact results after commit.
    """
    changed = {"resource_season": 0, "task_payload_season": 0}
    for record in records:
        if record.get("confidence") != "SAFE_INFER":
            continue
        # Phase 2D boundary: historical FAILED/CANCELLED/SUCCESS rows are audit
        # evidence only.  Canary preparation may touch only executable tasks.
        if record.get("task_status") not in {"QUEUED", "RETRY_WAIT"}:
            continue
        season = record.get("inferred_season")
        if not isinstance(season, int) or season < 1:
            continue
        resource_id = record.get("resource_id")
        if isinstance(resource_id, int):
            resource = await db.get(Resource, resource_id)
            if resource is not None and resource.season is None:
                resource.season = season
                changed["resource_season"] += 1
        task_id = record.get("task_id")
        if isinstance(task_id, int):
            task = await db.get(TransferQueueTask, task_id)
            if task is not None:
                payload = dict(task.payload or {})
                if payload.get("season") is None:
                    payload["season"] = season
                    task.payload = payload
                    changed["task_payload_season"] += 1
    await db.flush()
    return changed
