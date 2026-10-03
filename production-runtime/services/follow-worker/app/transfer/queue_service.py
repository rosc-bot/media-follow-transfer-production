from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.follow.episode_keys import canonical_episode_key
from app.transfer.quality_rank import extract_quality_score
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.transfer.errors import NON_RETRYABLE_CATEGORIES, TransferErrorCategory
from app.transfer.normalization import build_idempotency_key
from app.transfer.status import SUCCESS_TERMINAL_STATUSES, TransferStatus, is_execution_active_task


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
        incoming_score: int = 0,
        media_type: str | None = None,
    ) -> tuple[TransferQueueTask | None, bool]:
        if tmdb_id is None:
            return None, False

        rows = list((await db.scalars(
            select(TransferQueueTask)
            .where(TransferQueueTask.status.in_(SUCCESS_TERMINAL_STATUSES))
            .order_by(TransferQueueTask.id.desc())
        )).all())
        if not rows:
            return None, False

        resources: dict[int, Resource] = {}
        resource_ids = {row.resource_id for row in rows if row.resource_id is not None}
        if resource_ids:
            resources = {
                resource.id: resource
                for resource in (await db.scalars(
                    select(Resource).where(Resource.id.in_(resource_ids))
                )).all()
            }

        is_movie = (str(media_type or '').casefold() in {'movie', '电影'}) or (season is None and not episode_keys)

        if is_movie:
            # 电影跨链接去重与高码判断
            for row in rows:
                payload = dict(row.payload or {})
                resource = resources.get(row.resource_id)
                row_tmdb = payload.get('tmdb_id') or (resource.tmdb_id if resource else None)
                try:
                    if int(row_tmdb) == int(tmdb_id):
                        existing_score = 0
                        for val in [row.error_message, str(payload), resource.title if resource else '', str(resource.file_names if resource else '')]:
                            s = extract_quality_score(str(val or ''))
                            if s > existing_score:
                                existing_score = s
                        if existing_score >= incoming_score:
                            return row, True
                except (TypeError, ValueError):
                    continue
            return None, False

        # 电视剧按季和集数判断
        if season is None or not episode_keys:
            return None, False

        target = {
            key
            for value in episode_keys
            if (key := canonical_episode_key(season, value)) is not None
        }
        if not target:
            return None, False

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
                existing_score = 0
                for val in [row.error_message, str(payload), resource.title if resource else '', str(resource.file_names if resource else '')]:
                    s = extract_quality_score(str(val or ''))
                    if s > existing_score:
                        existing_score = s
                if existing_score >= incoming_score:
                    return row, True
        return None, False

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
            # Idempotent discovery is not an operator approval. Preserve review
            # holds and terminal failures instead of silently making them runnable.
            return EnqueueResult(
                existing,
                created=False,
                reused=is_execution_active_task(existing),
                deduplicated=is_success,
            )

        resource = await db.get(Resource, resource_id)
        tmdb_id = payload.get('tmdb_id') or (resource.tmdb_id if resource else None)
        season = payload.get('season') or (resource.season if resource else None)
        media_type = payload.get('media_type') or (resource.media_type if resource else None)

        incoming_score = 0
        for val in [str(payload), resource.title if resource else '', str(resource.file_names if resource else '')]:
            s = extract_quality_score(str(val or ''))
            if s > incoming_score:
                incoming_score = s

        existing_success, should_skip = await TransferQueueService._terminal_success_for_payload(
            db,
            tmdb_id=int(tmdb_id) if tmdb_id is not None else None,
            season=int(season) if season is not None else None,
            episode_keys=episode_keys or payload.get('episode_keys'),
            incoming_score=incoming_score,
            media_type=media_type,
        )
        if should_skip and existing_success is not None:
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
                    or_(
                        preflight_classification.is_(None),
                        func.upper(func.trim(preflight_classification)).not_in(['NEEDS_REVIEW', 'REJECTED']),
                    ),
                )
                .order_by(TransferQueueTask.priority.asc(), TransferQueueTask.id.desc())
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
