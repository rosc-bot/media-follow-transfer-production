import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.bot_settings import BotSettings
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
        db.add_all([BotSettings(key="global_pause", val="0"), BotSettings(key="transfer_paused", val="0")])
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
async def test_pending_review_row_is_not_reported_as_reused_active_transfer(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/pending-enqueue.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        review = await TransferQueueService.enqueue(
            db,
            resource_id=807,
            provider="guangya",
            payload={"episode_keys": ["S01E07"]},
            episode_keys=["S01E07"],
        )
        review.status = "PENDING"
        result = await TransferQueueService.enqueue_with_result(
            db,
            resource_id=807,
            provider="guangya",
            payload={"episode_keys": ["S01E07"]},
            episode_keys=["S01E07"],
        )

        assert result.task.id == review.id
        assert result.reused is False
        assert result.deduplicated is False
        assert result.task.status == "PENDING"
    await engine.dispose()


@pytest.mark.asyncio
async def test_worker_hydrates_missing_fields_from_resource_and_cloud_config(tmp_path, monkeypatch):
    from app.models.cloud import CloudConfig
    from app.models.resource import Resource
    from app.models.watchlist import SeriesWatchlist

    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/hydration.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        db.add_all([BotSettings(key="global_pause", val="0"), BotSettings(key="transfer_paused", val="0")])
        db.add(CloudConfig(
            name='guangya',
            auth_ref='auth-from-config',
            target_folder_id='target-from-config',
            ongoing_target_folder_id='ongoing-from-config',
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
        db.add(SeriesWatchlist(
            tmdb_id=12345,
            title='测试剧',
            season=1,
            status='FOLLOWING',
            collected_episodes=[],
        ))
        await TransferQueueService.enqueue(
            db,
            resource_id=100,
            provider='guangya',
            payload={
                'mode': 'reconcile',
                'tmdb_metadata': {
                    'id': 12345,
                    'media_type': 'tv',
                    'origin_country': ['CN'],
                    'original_language': 'zh',
                    'genres': [{'id': 18, 'name': 'Drama'}],
                    'metadata_complete': True,
                    'seasons': [{'season_number': 1}],
                },
            },  # Intentionally missing share_url, auth_token, target_folder_id
        )

    recorded_payload = {}

    class InspectingAdapter:
        async def transfer(self, payload):
            recorded_payload.update(payload)
            return TransferOutcome(True, True, payload.get('target_folder_id'), tuple(payload.get('expected_files') or []))

    async def fake_runtime_preflight(self, **kwargs):
        return kwargs["payload"]

    monkeypatch.setattr(TransferQueueWorker, "_runtime_final_preflight", fake_runtime_preflight)
    worker = TransferQueueWorker(sessions, TransferOrchestrator({'guangya': InspectingAdapter()}), worker_id='test-worker')
    assert await worker.process_once() is True

    assert recorded_payload['share_url'] == 'https://pan.guangyapan.com/s/hydrated'
    assert recorded_payload['auth_token'] == 'auth-from-config'
    assert recorded_payload['target_folder_id'] == 'ongoing-from-config'
    assert recorded_payload['expected_files'] == ['S01E01.mkv']
    assert recorded_payload['title'] == '测试剧'
    await engine.dispose()

