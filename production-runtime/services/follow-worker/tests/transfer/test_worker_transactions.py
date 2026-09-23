"""Phase 2A: transfer worker transaction boundaries.

Covers:
 14. claim transaction commits BEFORE remote IO executes
 15. remote failure is recorded in a separate failure transaction
 18. transfer_paused=true -> real transfer still zero claim (orchestrator untouched)
"""

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.bot_settings import BotSettings
from app.models.transfer import TransferQueueTask
from app.transfer.errors import TransferErrorCategory
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker
from app.transfer.status import TransferOutcome, TransferStatus


@pytest.fixture()
async def sessions(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/tx.db')
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    yield maker
    await engine.dispose()


class TransactionProbeAdapter:
    """Reads task state from a SEPARATE session inside remote IO to prove the
    claim transaction has already committed before any remote work starts."""

    def __init__(self, sessions):
        self.sessions = sessions
        self.task_id = None
        self.status_during_io = None
        self.invocations = 0

    async def transfer(self, payload):
        self.invocations += 1
        async with self.sessions() as db:
            task = await db.scalar(select(TransferQueueTask).order_by(TransferQueueTask.id))
            self.task_id = task.id
            self.status_during_io = task.status
        return TransferOutcome(True, True, 'folder-1', tuple(payload.get('expected_files') or []))


@pytest.mark.asyncio
async def test_claim_transaction_commits_before_remote_io(sessions):
    async with sessions() as db, db.begin():
        db.add_all([BotSettings(key='global_pause', val='0'), BotSettings(key='transfer_paused', val='0')])
        task = await TransferQueueService.enqueue(
            db, resource_id=7, provider='dry-run', episode_keys=['S01E01'], payload={'expected_files': ['a.mkv']},
        )
    probe = TransactionProbeAdapter(sessions)
    worker = TransferQueueWorker(sessions, TransferOrchestrator({'dry-run': probe}), worker_id='test')
    assert await worker.process_once() is True

    assert probe.task_id == task.id
    assert probe.status_during_io == TransferStatus.RUNNING  # visible to another session => claim committed
    async with sessions() as db:
        final_task = await db.get(TransferQueueTask, task.id)
        assert final_task.status == TransferStatus.COMPLETED


class ExplodingAdapter:
    def __init__(self, sessions, category=None):
        self.sessions = sessions
        self.category = category
        self.status_during_io = None

    async def transfer(self, payload):
        async with self.sessions() as db:
            task = await db.scalar(select(TransferQueueTask).order_by(TransferQueueTask.id))
            self.status_during_io = task.status
        raise RuntimeError('remote restore failed')


@pytest.mark.asyncio
async def test_remote_failure_is_recorded_in_separate_transaction(sessions):
    async with sessions() as db, db.begin():
        db.add_all([BotSettings(key='global_pause', val='0'), BotSettings(key='transfer_paused', val='0')])
        task = await TransferQueueService.enqueue(
            db, resource_id=8, provider='dry-run', episode_keys=['S01E01'], payload={'expected_files': ['a.mkv']},
        )
    adapter = ExplodingAdapter(sessions)
    worker = TransferQueueWorker(sessions, TransferOrchestrator({'dry-run': adapter}), worker_id='test')
    assert await worker.process_once() is True

    assert adapter.status_during_io == TransferStatus.RUNNING  # claim was committed, failure is separate
    async with sessions() as db:
        final_task = await db.get(TransferQueueTask, task.id)
        assert final_task.status == TransferStatus.RETRY_WAIT  # UNKNOWN -> retryable with backoff
        assert '[UNKNOWN]' in (final_task.error_message or '')
        assert final_task.locked_at is None
        assert final_task.locked_by is None


@pytest.mark.asyncio
async def test_worker_classifies_remote_errors_by_category(sessions):
    from app.transfer.errors import GuangyaTransferError, NoVideoFilesError

    async with sessions() as db, db.begin():
        db.add_all([BotSettings(key='global_pause', val='0'), BotSettings(key='transfer_paused', val='0')])
        first = await TransferQueueService.enqueue(
            db, resource_id=9, provider='dry-run', episode_keys=['S01E01'], payload={'expected_files': ['a.mkv']},
        )
        second = await TransferQueueService.enqueue(
            db, resource_id=10, provider='dry-run', episode_keys=['S01E02'], payload={'expected_files': ['b.mkv']},
        )

    class CategoryAdapter:
        def __init__(self, error):
            self.error = error

        async def transfer(self, payload):
            raise self.error

    # 1) NO_VIDEO_FILES must be terminal for this resource (no retry starvation)
    worker1 = TransferQueueWorker(
        sessions,
        TransferOrchestrator({'dry-run': CategoryAdapter(NoVideoFilesError('no videos'))}),
        worker_id='test-1',
    )
    assert await worker1.process_once() is True
    async with sessions() as db:
        failed_task = await db.get(TransferQueueTask, first.id)
        assert failed_task.status == TransferStatus.FAILED
        assert '[NO_VIDEO_FILES]' in failed_task.error_message

    # 2) RATE_LIMITED must keep the task retryable with backoff
    worker2 = TransferQueueWorker(
        sessions,
        TransferOrchestrator({'dry-run': CategoryAdapter(GuangyaTransferError(TransferErrorCategory.RATE_LIMITED, 'too fast'))}),
        worker_id='test-2',
    )
    assert await worker2.process_once() is True
    async with sessions() as db:
        retry_task = await db.get(TransferQueueTask, second.id)
        assert retry_task.status == TransferStatus.RETRY_WAIT
        assert retry_task.next_run_at is not None


@pytest.mark.asyncio
async def test_transfer_paused_means_zero_orchestrator_invocations(sessions):
    async with sessions() as db, db.begin():
        db.add_all([BotSettings(key='global_pause', val='0'), BotSettings(key='transfer_paused', val='1')])
        await TransferQueueService.enqueue(
            db, resource_id=11, provider='dry-run', episode_keys=['S01E01'], payload={'expected_files': ['a.mkv']},
        )
    calls = []

    class NeverCalledAdapter:
        async def transfer(self, payload):
            calls.append(1)
            return TransferOutcome(True, True, 'f', ('a.mkv',))

    worker = TransferQueueWorker(sessions, TransferOrchestrator({'dry-run': NeverCalledAdapter()}), worker_id='test')
    assert await worker.process_once() is False
    assert calls == []  # zero claim -> zero remote work
    async with sessions() as db:
        task = await db.scalar(select(TransferQueueTask))
        assert task.status == TransferStatus.QUEUED  # untouched
