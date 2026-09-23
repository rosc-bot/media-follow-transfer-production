from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.transfer import TransferQueueTask
from app.transfer.errors import NON_RETRYABLE_CATEGORIES, TransferErrorCategory
from app.transfer.normalization import build_idempotency_key
from app.transfer.status import TransferStatus


class TransferQueueService:
    @staticmethod
    async def enqueue(db: AsyncSession, *, resource_id: int, provider: str, payload: dict, episode_keys: list[str] | None = None, priority: int = 100) -> TransferQueueTask:
        key = build_idempotency_key(resource_id, provider, episode_keys)
        existing = await db.scalar(select(TransferQueueTask).where(TransferQueueTask.idempotency_key == key))
        if existing:
            return existing
        task = TransferQueueTask(resource_id=resource_id, idempotency_key=key, payload={**payload, 'provider': provider}, priority=priority)
        db.add(task)
        await db.flush()
        return task

    @staticmethod
    async def claim_next(db: AsyncSession, *, worker_id: str) -> TransferQueueTask | None:
        now = datetime.now(UTC)
        stmt = (select(TransferQueueTask)
                .where(TransferQueueTask.status.in_([TransferStatus.QUEUED, TransferStatus.RETRY_WAIT]), TransferQueueTask.next_run_at <= now)
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
