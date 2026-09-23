from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.follow.episode_keys import canonical_episode_key
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.transfer.errors import NON_RETRYABLE_CATEGORIES, TransferErrorCategory
from app.transfer.normalization import build_idempotency_key
from app.transfer.status import SUCCESS_TERMINAL_STATUSES, TransferStatus


@dataclass(frozen=True)
class EnqueueResult:
    task: TransferQueueTask
    created: bool
    reused: bool = False
    deduplicated: bool = False


class TransferQueueService:
    @staticmethod
    async def _terminal_success_for_payload(
        db: AsyncSession,
        *,
        tmdb_id: int | None,
        season: int | None,
        episode_keys: list[str] | None,
    ) -> TransferQueueTask | None:
        if tmdb_id is None or season is None or not episode_keys:
            return None
        target = {
            key
            for value in episode_keys
            if (key := canonical_episode_key(season, value)) is not None
        }
        if not target:
            return None
        rows = list((await db.scalars(
            select(TransferQueueTask)
            .where(TransferQueueTask.status.in_(SUCCESS_TERMINAL_STATUSES))
            .order_by(TransferQueueTask.id.asc())
        )).all())
        resources: dict[int, Resource] = {}
        resource_ids = {row.resource_id for row in rows if row.resource_id is not None}
        if resource_ids:
            resources = {
                resource.id: resource
                for resource in (await db.scalars(
                    select(Resource).where(Resource.id.in_(resource_ids))
                )).all()
            }
        for row in rows:
            payload = dict(row.payload or {})
            resource = resources.get(row.resource_id)
            row_tmdb = payload.get('tmdb_id') or (resource.tmdb_id if resource else None)
            row_season = payload.get('season') or (resource.season if resource else None)
            try:
                same_identity = int(row_tmdb) == int(tmdb_id) and int(row_season) == int(season)
            except (TypeError, ValueError):
                same_identity = False
            if not same_identity:
                continue
            raw_keys = payload.get('episode_keys') or ([resource.episode_key] if resource and resource.episode_key else [])
            row_keys = {
                key
                for value in raw_keys
                if (key := canonical_episode_key(int(row_season), value)) is not None
            }
            if row_keys & target:
                return row
        return None

    @staticmethod
    async def enqueue_with_result(
        db: AsyncSession,
        *,
        resource_id: int,
        provider: str,
        payload: dict[str, Any],
        episode_keys: list[str] | None = None,
        priority: int = 100,
    ) -> EnqueueResult:
        key = build_idempotency_key(resource_id, provider, episode_keys)
        existing = await db.scalar(select(TransferQueueTask).where(TransferQueueTask.idempotency_key == key))
        if existing is not None:
            is_success = str(existing.status) in {str(value) for value in SUCCESS_TERMINAL_STATUSES}
            return EnqueueResult(existing, created=False, reused=not is_success, deduplicated=is_success)

        resource = await db.get(Resource, resource_id)
        tmdb_id = payload.get('tmdb_id') or (resource.tmdb_id if resource else None)
        season = payload.get('season') or (resource.season if resource else None)
        existing_success = await TransferQueueService._terminal_success_for_payload(
            db,
            tmdb_id=int(tmdb_id) if tmdb_id is not None else None,
            season=int(season) if season is not None else None,
            episode_keys=episode_keys or payload.get('episode_keys'),
        )
        if existing_success is not None:
            return EnqueueResult(existing_success, created=False, reused=False, deduplicated=True)

        task = TransferQueueTask(
            resource_id=resource_id,
            idempotency_key=key,
            payload={**payload, 'provider': provider},
            priority=priority,
        )
        db.add(task)
        await db.flush()
        return EnqueueResult(task, created=True)

    @staticmethod
    async def enqueue(db: AsyncSession, *, resource_id: int, provider: str, payload: dict, episode_keys: list[str] | None = None, priority: int = 100) -> TransferQueueTask:
        return (await TransferQueueService.enqueue_with_result(
            db,
            resource_id=resource_id,
            provider=provider,
            payload=payload,
            episode_keys=episode_keys,
            priority=priority,
        )).task

    @staticmethod
    async def status_counts(db: AsyncSession) -> dict[str, int]:
        rows = (await db.execute(
            select(TransferQueueTask.status, func.count(TransferQueueTask.id))
            .group_by(TransferQueueTask.status)
        )).all()
        return {str(status): int(count) for status, count in rows}

    @staticmethod
    async def claim_next(db: AsyncSession, *, worker_id: str) -> TransferQueueTask | None:
        now = datetime.now(UTC)
        preflight_classification = TransferQueueTask.payload['preflight_classification'].as_string()
        stmt = (select(TransferQueueTask)
                .where(
                    TransferQueueTask.status.in_([TransferStatus.QUEUED, TransferStatus.RETRY_WAIT]),
                    TransferQueueTask.next_run_at <= now,
                    or_(preflight_classification.is_(None), preflight_classification == 'AUTO_SAFE'),
                )
                .order_by(TransferQueueTask.priority.asc(), TransferQueueTask.id.asc())
                .with_for_update(skip_locked=True).limit(1))
        task = (await db.execute(stmt)).scalar_one_or_none()
        if not task:
            return None
        task.status = TransferStatus.RUNNING
        task.locked_at = now
        task.locked_by = worker_id
        task.attempt_count += 1
        await db.flush()
        return task

    @staticmethod
    async def mark_completed(db: AsyncSession, task: TransferQueueTask, result: dict) -> None:
        task.status = TransferStatus.COMPLETED
        task.result = result
        task.error_message = None
        task.locked_at = None
        task.locked_by = None
        await db.flush()

    @staticmethod
    async def mark_failed(db: AsyncSession, task: TransferQueueTask, error: str, *, category: TransferErrorCategory | None = None) -> None:
        """Record a failure.

        Categorized terminal failures (including AUTH_EXPIRED) land straight in
        FAILED and are never re-scheduled for the ordinary exponential retry.
        """
        prefix = f'[{category}] ' if category else ''
        task.error_message = f'{prefix}{error}'[:4000]
        task.locked_at = None
        task.locked_by = None
        if category is not None and category in NON_RETRYABLE_CATEGORIES:
            task.status = TransferStatus.FAILED  # never re-scheduled for retry
        elif task.attempt_count < task.max_retries:
            task.status = TransferStatus.RETRY_WAIT
            task.next_run_at = datetime.now(UTC) + timedelta(seconds=min(3600, 2 ** task.attempt_count))
        else:
            task.status = TransferStatus.FAILED
        await db.flush()

    @staticmethod
    async def recover_stale(db: AsyncSession, *, stale_after_seconds: int = 900) -> int:
        cutoff = datetime.now(UTC) - timedelta(seconds=stale_after_seconds)
        result = await db.execute(select(TransferQueueTask).where(TransferQueueTask.status == TransferStatus.RUNNING, TransferQueueTask.locked_at < cutoff))
        tasks = list(result.scalars())
        for task in tasks:
            task.status = TransferStatus.RETRY_WAIT
            task.locked_at = None
            task.locked_by = None
        await db.flush()
        return len(tasks)
