"""Read-only historic season audit across resources, queues and ingest rows."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ingest import ChannelIngestJob
from app.models.resource import Resource
from app.models.resource_candidate import ResourceCandidate
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.season_backfill import build_audit_record


def _payload_season(payload: object) -> int | str | None:
    return payload.get("season") if isinstance(payload, dict) else None


def _episode_keys(value: object) -> list[str | None]:
    if isinstance(value, list) and value:
        return [str(item) if item is not None else None for item in value]
    return [None]


async def audit_missing_seasons(db: AsyncSession) -> dict[str, Any]:
    """Scan all four required sources without mutating the database."""
    watchlist_by_identity: dict[tuple[int, str], set[int]] = defaultdict(set)
    for row in (await db.execute(select(SeriesWatchlist.tmdb_id, SeriesWatchlist.title, SeriesWatchlist.season))).all():
        watchlist_by_identity[(int(row.tmdb_id), str(row.title))].add(int(row.season))

    candidate_by_episode: dict[tuple[int, str], set[int]] = defaultdict(set)
    for row in (await db.execute(select(
        ResourceCandidate.tmdb_id, ResourceCandidate.episode_key, ResourceCandidate.season,
    ))).all():
        candidate_by_episode[(int(row.tmdb_id), str(row.episode_key))].add(int(row.season))

    records: list[dict[str, Any]] = []
    task_resource_ids: set[int] = set()
    tasks = (await db.execute(
        select(TransferQueueTask, Resource).outerjoin(Resource, TransferQueueTask.resource_id == Resource.id)
    )).all()
    for task, resource in tasks:
        if resource is not None:
            task_resource_ids.add(resource.id)
        payload_season = _payload_season(task.payload)
        resource_season = resource.season if resource is not None else None
        if payload_season is not None and resource_season is not None:
            continue
        tmdb_id = resource.tmdb_id if resource is not None else None
        title = resource.title if resource is not None else None
        episode_key = (
            (task.payload or {}).get("episode_key")
            or next(iter((task.payload or {}).get("episode_keys") or [None]))
            or (resource.episode_key if resource is not None else None)
        )
        watchlist_seasons = watchlist_by_identity.get((int(tmdb_id), str(title)), set()) if tmdb_id and title else set()
        candidate_seasons = candidate_by_episode.get((int(tmdb_id), str(episode_key)), set()) if tmdb_id and episode_key else set()
        record = build_audit_record(
            task_id=task.id,
            resource_id=resource.id if resource is not None else task.resource_id,
            tmdb_id=tmdb_id,
            title=title,
            episode_key=episode_key,
            payload_season=payload_season,
            resource_season=resource_season,
            watchlist_seasons=watchlist_seasons,
            candidate_seasons=candidate_seasons,
            source_type=resource.source_type if resource is not None else None,
        )
        record.update({"record_type": "transfer_task", "task_status": task.status})
        records.append(record)

    standalone = (await db.scalars(select(Resource).where(Resource.season.is_(None)))).all()
    for resource in standalone:
        if resource.id in task_resource_ids:
            continue
        watchlist_seasons = watchlist_by_identity.get((int(resource.tmdb_id), str(resource.title)), set()) if resource.tmdb_id and resource.title else set()
        candidate_seasons = candidate_by_episode.get((int(resource.tmdb_id), str(resource.episode_key)), set()) if resource.tmdb_id and resource.episode_key else set()
        record = build_audit_record(
            task_id=None, resource_id=resource.id, tmdb_id=resource.tmdb_id, title=resource.title,
            episode_key=resource.episode_key, payload_season=None, resource_season=None,
            watchlist_seasons=watchlist_seasons, candidate_seasons=candidate_seasons,
            source_type=resource.source_type,
        )
        record.update({"record_type": "resource", "task_status": None})
        records.append(record)

    ingest_rows = (await db.scalars(select(ChannelIngestJob).where(ChannelIngestJob.season.is_(None)))).all()
    for job in ingest_rows:
        parsed = dict(job.parsed_data or {})
        for episode_key in _episode_keys(job.detected_episodes or parsed.get("episode_keys")):
            watchlist_seasons = watchlist_by_identity.get((int(job.tmdb_id), str(job.title)), set()) if job.tmdb_id and job.title else set()
            candidate_seasons = candidate_by_episode.get((int(job.tmdb_id), str(episode_key)), set()) if job.tmdb_id and episode_key else set()
            record = build_audit_record(
                task_id=None, resource_id=None, tmdb_id=job.tmdb_id, title=job.title,
                episode_key=episode_key, payload_season=_payload_season(parsed), resource_season=job.season,
                watchlist_seasons=watchlist_seasons, candidate_seasons=candidate_seasons,
                source_type=job.source_type,
            )
            record.update({"record_type": "channel_ingest_job", "ingest_job_id": job.id, "task_status": None})
            records.append(record)

    candidate_null_count = await db.scalar(select(func.count()).select_from(ResourceCandidate).where(ResourceCandidate.season.is_(None)))
    counts = Counter(str(record["confidence"]) for record in records)
    by_task_status = Counter(
        str(record["task_status"]) for record in records if record.get("record_type") == "transfer_task"
    )
    return {
        "TOTAL_MISSING_SEASON": len(records),
        "SAFE_INFER": counts["SAFE_INFER"],
        "CONFLICT": sum(1 for record in records if record.get("conflict")),
        "NO_EVIDENCE": sum(1 for record in records if record.get("evidence") == ["episode_key:unparseable"]),
        "NEEDS_REVIEW": counts["NEEDS_REVIEW"],
        "by_task_status": dict(sorted(by_task_status.items())),
        "resource_candidates_season_null": int(candidate_null_count or 0),
        "records": records,
    }
