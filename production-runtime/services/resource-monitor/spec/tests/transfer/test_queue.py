import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.transfer import TransferQueueTask
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker
from app.transfer.status import TransferOutcome, TransferStatus


class FakeAdapter:
    async def transfer(self, payload):
        return TransferOutcome(True, True, 'folder-1', tuple(payload.get('expected_files', [])))


@pytest.mark.asyncio
async def test_enqueue_is_idempotent_and_worker_verifies(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/transfer.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        first = await TransferQueueService.enqueue(db, resource_id=7, provider='dry-run', episode_keys=['S01E01'], payload={'expected_files': ['a.mkv']})
        second = await TransferQueueService.enqueue(db, resource_id=7, provider='dry-run', episode_keys=['S01E01'], payload={'expected_files': ['a.mkv']})
        assert first.id == second.id
    worker = TransferQueueWorker(sessions, TransferOrchestrator({'dry-run': FakeAdapter()}), worker_id='test')
    assert await worker.process_once() is True
    async with sessions() as db:
        task = await db.scalar(select(TransferQueueTask).where(TransferQueueTask.id == first.id))
        assert task.status == TransferStatus.COMPLETED
        assert task.result['verified'] is True
    await engine.dispose()


@pytest.mark.asyncio
async def test_worker_hydrates_missing_fields_from_resource_and_cloud_config(tmp_path):
    from app.models.cloud import CloudConfig
    from app.models.resource import Resource

    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/hydration.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        db.add(CloudConfig(
            name='guangya',
            auth_ref='auth-from-config',
            target_folder_id='target-from-config',
            enabled=True,
        ))
        db.add(Resource(
            id=100,
            identity_key='ident-100',
            tmdb_id=12345,
            title='测试剧',
            season=1,
            episode=1,
            episode_key='E01',
            share_url='https://pan.guangyapan.com/s/hydrated',
            cloud_name='guangya',
            source_type='telegram_channel',
            file_names=['S01E01.mkv'],
        ))
        await TransferQueueService.enqueue(
            db,
            resource_id=100,
            provider='guangya',
            payload={'mode': 'reconcile'},  # Intentionally missing share_url, auth_token, target_folder_id
        )

    recorded_payload = {}

    class InspectingAdapter:
        async def transfer(self, payload):
            recorded_payload.update(payload)
            return TransferOutcome(True, True, payload.get('target_folder_id'), tuple(payload.get('expected_files') or []))

    worker = TransferQueueWorker(sessions, TransferOrchestrator({'guangya': InspectingAdapter()}), worker_id='test-worker')
    assert await worker.process_once() is True

    assert recorded_payload['share_url'] == 'https://pan.guangyapan.com/s/hydrated'
    assert recorded_payload['auth_token'] == 'auth-from-config'
    assert recorded_payload['target_folder_id'] == 'target-from-config'
    assert recorded_payload['expected_files'] == ['S01E01.mkv']
    assert recorded_payload['title'] == '测试剧'
    await engine.dispose()

