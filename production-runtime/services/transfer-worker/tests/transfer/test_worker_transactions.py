"""Phase 2A: transfer worker transaction boundaries.

Covers:
 14. claim transaction commits BEFORE remote IO executes
 15. remote failure is recorded in a separate failure transaction
 18. transfer_paused=true -> real transfer still zero claim (orchestrator untouched)
"""

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.bot_settings import BotSettings
from app.models.cloud import CloudDiskInventory
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
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


@pytest.mark.asyncio
async def test_verified_batch_records_each_episode_in_inventory_collected_and_resource(sessions):
    file_e01 = "作品 {tmdbid-7}.S01E01.720p.WEB-DL.mkv"
    file_e02 = "作品 {tmdbid-7}.S01E02.1080p.WEB-DL.mkv"
    file_e03 = "作品 {tmdbid-7}.S01E03.1080p.WEB-DL.mkv"
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key='global_pause', val='0'),
            BotSettings(key='transfer_paused', val='0'),
            SeriesWatchlist(
                tmdb_id=7, title='作品', season=1, status='FOLLOWING',
                collected_episodes=['S01E01'],
            ),
            CloudDiskInventory(
                title='作品', clean_title='作品', tmdb_id=7, season=1, episode=1,
                file_name=file_e01, rel_path=f'旧路径/{file_e01}',
            ),
            Resource(
                id=12, identity_key='batch-12', tmdb_id=7, title='作品', media_type='tv', season=1,
                episode=2, episode_key='S01E02', cloud_name='dry-run',
                share_url='https://example.invalid/batch', source_type='watchlist_scout', file_names=[file_e01],
            ),
        ])
        task = await TransferQueueService.enqueue(
            db,
            resource_id=12,
            provider='dry-run',
            episode_keys=['S01E02', 'S01E03'],
            payload={
                'selection_mode': 'MISSING_EPISODES',
                'episode_keys': ['S01E02', 'S01E03'],
                'selected_episode_keys': ['S01E02', 'S01E03'],
                'selected_file_names': [file_e02, file_e03],
                'expected_files': [file_e02, file_e03],
                'selected_file_sizes': {file_e02: 100, file_e03: 200},
                'remote_rel_path_prefix': '电视剧/日番/作品 {tmdbid-7}',
            },
        )

    class BatchAdapter:
        async def transfer(self, payload):
            records = [
                {'file_id': 'cloud-e02', 'name': file_e02, 'size': 100},
                {'file_id': 'cloud-e03', 'name': file_e03, 'size': 200},
            ]
            episodes = [
                {'episode_key': 'S01E02', **records[0]},
                {'episode_key': 'S01E03', **records[1]},
            ]
            payload['verified_episode_files'] = episodes
            return TransferOutcome(
                True,
                True,
                'season-folder',
                (file_e02, file_e03),
                remote_file_records=tuple(records),
                verified_episode_files=tuple(episodes),
            )

    worker = TransferQueueWorker(
        sessions,
        TransferOrchestrator({'dry-run': BatchAdapter()}),
        worker_id='batch-finalizer-test',
    )
    assert await worker.process_once() is True
    async with sessions() as db:
        final_task = await db.get(TransferQueueTask, task.id)
        resource = await db.get(Resource, 12)
        watchlist = await db.scalar(select(SeriesWatchlist).where(SeriesWatchlist.tmdb_id == 7))
        inventory = list((await db.scalars(
            select(CloudDiskInventory).where(CloudDiskInventory.tmdb_id == 7).order_by(CloudDiskInventory.episode)
        )).all())
        assert final_task.status == TransferStatus.COMPLETED
        assert final_task.result['selected_episode_keys'] == ['S01E02', 'S01E03']
        assert final_task.result['inventory']['written'] == 2
        assert [row.episode for row in inventory] == [1, 2, 3]
        assert [row.rel_path for row in inventory] == [
            f'旧路径/{file_e01}',
            f'电视剧/日番/作品 {{tmdbid-7}}/{file_e02}',
            f'电视剧/日番/作品 {{tmdbid-7}}/{file_e03}',
        ]
        assert watchlist.collected_episodes == ['S01E01', 'S01E02', 'S01E03']
        assert resource.file_names == [file_e01, file_e02, file_e03]


@pytest.mark.asyncio
async def test_invalid_batch_episode_readback_pauses_before_completion(sessions):
    file_e02 = "作品 {tmdbid-7}.S01E02.1080p.WEB-DL.mkv"
    file_e03 = "作品 {tmdbid-7}.S01E03.1080p.WEB-DL.mkv"
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key='global_pause', val='0'),
            BotSettings(key='transfer_paused', val='0'),
            SeriesWatchlist(tmdb_id=7, title='作品', season=1, status='FOLLOWING', collected_episodes=[]),
            Resource(
                id=13, identity_key='batch-13', tmdb_id=7, title='作品', media_type='tv', season=1,
                episode=2, episode_key='S01E02', cloud_name='dry-run',
                share_url='https://example.invalid/batch', source_type='watchlist_scout', file_names=[],
            ),
        ])
        task = await TransferQueueService.enqueue(
            db, resource_id=13, provider='dry-run', episode_keys=['S01E02', 'S01E03'],
            payload={
                'selection_mode': 'MISSING_EPISODES',
                'episode_keys': ['S01E02', 'S01E03'],
                'selected_episode_keys': ['S01E02', 'S01E03'],
                'selected_file_names': [file_e02, file_e03],
                'expected_files': [file_e02, file_e03],
            },
        )
    calls = []

    class InvalidBatchAdapter:
        async def transfer(self, payload):
            calls.append(1)
            one = {'episode_key': 'S01E02', 'file_id': 'cloud-e02', 'file_name': file_e02, 'size': 100}
            return TransferOutcome(
                True, True, 'season-folder', (file_e02, file_e03),
                remote_file_records=({'file_id': 'cloud-e02', 'name': file_e02}, {'file_id': 'cloud-e03', 'name': file_e03}),
                verified_episode_files=(one,),
            )

    worker = TransferQueueWorker(
        sessions, TransferOrchestrator({'dry-run': InvalidBatchAdapter()}), worker_id='bad-batch-test',
    )
    assert await worker.process_once() is False
    assert await worker.process_once() is False
    assert calls == [1]
    async with sessions() as db:
        final_task = await db.get(TransferQueueTask, task.id)
        pause = await db.get(BotSettings, 'transfer_paused')
        inventory_count = await db.scalar(select(func.count(CloudDiskInventory.id)))
        assert final_task.status == TransferStatus.RETRY_WAIT
        assert final_task.result['integrity_error'] == 'VERIFIED_EPISODE_MAP_INCOMPLETE'
        assert pause.val == '1'
        assert inventory_count == 0


@pytest.mark.asyncio
async def test_scope_violation_failure_pauses_and_holds_task_for_review(sessions):
    from app.transfer.errors import GuangyaTransferError

    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key='global_pause', val='0'),
            BotSettings(key='transfer_paused', val='0'),
        ])
        task = await TransferQueueService.enqueue(
            db, resource_id=14, provider='dry-run', episode_keys=['S01E02'],
            payload={'episode_keys': ['S01E02'], 'expected_files': ['S01E02.mkv']},
        )
    calls = []

    class ScopeViolationAdapter:
        async def transfer(self, payload):
            calls.append(1)
            raise GuangyaTransferError(
                TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
                'readback observed an unexpected episode',
            )

    worker = TransferQueueWorker(
        sessions,
        TransferOrchestrator({'dry-run': ScopeViolationAdapter()}),
        worker_id='scope-violation-test',
    )
    assert await worker.process_once() is False
    assert await worker.process_once() is False
    assert calls == [1]
    async with sessions() as db:
        final_task = await db.get(TransferQueueTask, task.id)
        pause = await db.get(BotSettings, 'transfer_paused')
        assert final_task.status == 'PENDING'
        assert final_task.payload['scope_integrity_hold'] is True
        assert final_task.payload['preflight_classification'] == 'NEEDS_REVIEW'
        assert pause.val == '1'
