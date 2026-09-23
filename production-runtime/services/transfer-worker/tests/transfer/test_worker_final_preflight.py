import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.bot_settings import BotSettings
from app.models.cloud import CloudConfig
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker
from app.transfer.status import TransferOutcome


@pytest.mark.asyncio
async def test_worker_local_final_preflight_blocks_collected_episode_before_adapter(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/worker-preflight.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="0"), BotSettings(key="transfer_paused", val="0"),
            CloudConfig(name="guangya", auth_ref="auth", target_folder_id="completed", ongoing_target_folder_id="ongoing", enabled=True),
            SeriesWatchlist(tmdb_id=9, title="已收剧", season=1, status="FOLLOWING", collected_episodes=["S01E02"]),
            Resource(
                id=9, identity_key="preflight-9", tmdb_id=9, title="已收剧", media_type="tv", season=1,
                episode=2, episode_key="S01E02", share_url="https://example/s/9", cloud_name="guangya",
                source_type="watchlist_scout", file_names=["S01E02.mkv"],
            ),
        ])
        await TransferQueueService.enqueue(
            db, resource_id=9, provider="guangya", episode_keys=["S01E02"],
            payload={
                "tmdb_metadata": {
                    "id": 9, "media_type": "tv", "origin_country": ["CN"], "original_language": "zh",
                    "genres": [{"id": 18, "name": "Drama"}], "metadata_complete": True,
                    "seasons": [{"season_number": 1}],
                },
            },
        )
    calls = []

    class Adapter:
        async def transfer(self, payload):
            calls.append(payload)
            return TransferOutcome(True, True, "folder", ("S01E02.mkv",))

    worker = TransferQueueWorker(sessions, TransferOrchestrator({"guangya": Adapter()}), worker_id="test")

    async def blocked_batch_preflight(_db, *, task, resource, payload):
        return {"classification": "REJECTED", "reason": "ALREADY_COLLECTED"}

    worker._batch_episode_presence_preflight = blocked_batch_preflight
    assert await worker.process_once() is False
    assert calls == []
    async with sessions() as db:
        task = await db.scalar(db.query(TransferQueueTask)) if False else (await db.get(TransferQueueTask, 1))
        assert task.status == "PENDING"
        assert "ALREADY_COLLECTED" in (task.error_message or "")
    await engine.dispose()


@pytest.mark.asyncio
async def test_worker_runtime_final_preflight_blocks_incomplete_source_rename_before_adapter(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/runtime-preflight.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="0"), BotSettings(key="transfer_paused", val="0"),
            CloudConfig(name="guangya", auth_ref="auth", target_folder_id="completed", ongoing_target_folder_id="ongoing", enabled=True),
            SeriesWatchlist(tmdb_id=10, title="命名门禁", season=1, status="FOLLOWING", collected_episodes=[]),
            Resource(
                id=10, identity_key="runtime-preflight-10", tmdb_id=10, title="命名门禁", media_type="tv", season=1,
                episode=2, episode_key="S01E02", share_url="https://example/s/10", cloud_name="guangya",
                source_type="watchlist_scout", file_names=["S01E02.mkv"],
            ),
        ])
        await TransferQueueService.enqueue(
            db, resource_id=10, provider="guangya", episode_keys=["S01E02"],
            payload={
                "tmdb_metadata": {
                    "id": 10, "media_type": "tv", "origin_country": ["CN"], "original_language": "zh",
                    "genres": [{"id": 18, "name": "Drama"}], "metadata_complete": True,
                    "seasons": [{"season_number": 1}],
                },
            },
        )

    async def fake_preflight(*_args, **_kwargs):
        checks = {
            name: {"result": "PASS", "failure_code": None, "detail": "ok"}
            for name in (
                "check_task_status", "check_resource", "check_watchlist", "check_collected",
                "check_cloud_inventory", "check_duplicate_success", "check_duplicate_active",
                "check_share_url", "check_share_access", "check_video_files", "check_episode_match",
                "check_destination", "check_account_auth", "check_candidate_status", "check_source_type",
            )
        }
        return {
            "preflight_status": "CANARY_SAFE", "checks": checks,
            "remote_validation": {
                "selection_mode": "MISSING_EPISODES", "selected_file_ids": ["f2"],
                "selected_file_names": ["S01E02.mkv"],
                "selected_episode_keys": ["S01E02"],
                "episode_file_map": {"S01E02": "f2"},
                "share": {"video_files": []},
            },
        }

    async def auto_batch_preflight(_self, _db, *, task, resource, payload):
        return {
            "classification": "AUTO_SAFE", "reason": "TEST_SAFE",
            "missing_episode_keys": ["S01E02"], "share_episode_keys": ["S01E02"],
        }

    monkeypatch.setattr("app.transfer.queue_worker.preflight_task", fake_preflight)
    monkeypatch.setattr("app.transfer.queue_worker.TransferQueueWorker._batch_episode_presence_preflight", auto_batch_preflight)
    calls = []

    class Adapter:
        async def transfer(self, payload):
            calls.append(payload)
            return TransferOutcome(True, True, "folder", ("S01E02.mkv",))

    worker = TransferQueueWorker(sessions, TransferOrchestrator({"guangya": Adapter()}), worker_id="test-runtime")
    assert await worker.process_once() is False
    assert calls == []
    async with sessions() as db:
        task = await db.get(TransferQueueTask, 1)
        assert task.status == "PENDING"
        assert task.payload["preflight_classification"] == "NEEDS_REVIEW"
        assert task.payload["preflight_reason"] == "RENAME_PLAN_INCOMPLETE"
    await engine.dispose()


@pytest.mark.asyncio
async def test_worker_runtime_final_preflight_persists_exact_selection_snapshot(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/runtime-preflight-safe.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="0"), BotSettings(key="transfer_paused", val="0"),
            CloudConfig(name="guangya", auth_ref="auth", target_folder_id="completed", ongoing_target_folder_id="ongoing", enabled=True),
            SeriesWatchlist(tmdb_id=11, title="精确门禁", season=1, status="FOLLOWING", collected_episodes=[]),
            Resource(
                id=11, identity_key="runtime-preflight-11", tmdb_id=11, title="精确门禁", media_type="tv", season=1,
                episode=2, episode_key="S01E02", share_url="https://example/s/11", cloud_name="guangya",
                source_type="watchlist_scout", file_names=["S01E02.mkv"],
            ),
        ])
        await TransferQueueService.enqueue(
            db, resource_id=11, provider="guangya", episode_keys=["S01E02"],
            payload={
                "tmdb_metadata": {
                    "id": 11, "media_type": "tv", "origin_country": ["CN"], "original_language": "zh",
                    "genres": [{"id": 18, "name": "Drama"}], "metadata_complete": True,
                    "seasons": [{"season_number": 1}],
                },
            },
        )

    async def fake_preflight(*_args, **_kwargs):
        checks = {
            name: {"result": "PASS", "failure_code": None, "detail": "ok"}
            for name in (
                "check_task_status", "check_resource", "check_watchlist", "check_collected",
                "check_cloud_inventory", "check_duplicate_success", "check_duplicate_active",
                "check_share_url", "check_share_access", "check_video_files", "check_episode_match",
                "check_destination", "check_account_auth", "check_candidate_status", "check_source_type",
            )
        }
        selected = {"file_id": "f2", "name": "S01E02-Gyy.mkv"}
        return {
            "preflight_status": "CANARY_SAFE", "checks": checks,
            "remote_validation": {
                "selection_mode": "MISSING_EPISODES", "selected_file_ids": ["f2"],
                "selected_file_names": ["S01E02-Gyy.mkv"],
                "selected_episode_keys": ["S01E02"],
                "episode_file_map": {"S01E02": "f2"},
                "share": {"video_files": [selected, dict(selected)]},
            },
        }

    async def auto_batch_preflight(_self, _db, *, task, resource, payload):
        return {
            "classification": "AUTO_SAFE", "reason": "TEST_SAFE",
            "missing_episode_keys": ["S01E02"], "share_episode_keys": ["S01E02"],
        }

    monkeypatch.setattr("app.transfer.queue_worker.preflight_task", fake_preflight)
    monkeypatch.setattr("app.transfer.queue_worker.TransferQueueWorker._batch_episode_presence_preflight", auto_batch_preflight)
    worker = TransferQueueWorker(sessions, worker_id="test-runtime-safe")
    claimed = await worker._claim_payload()
    assert claimed is not None
    task_id, resource_id, payload = claimed
    final_payload = await worker._runtime_final_preflight(
        task_id=task_id,
        resource_id=resource_id,
        payload=payload,
    )
    assert final_payload is not None
    assert final_payload["selection_snapshot"] == {
        "selection_mode": "MISSING_EPISODES",
        "selected_file_ids": ["f2"],
        "selected_file_names": ["S01E02-Gyy.mkv"],
        "selected_episode_keys": ["S01E02"],
        "episode_file_map": {"S01E02": "f2"},
    }
    async with sessions() as db:
        task = await db.get(TransferQueueTask, 1)
        assert task.status == "RUNNING"
        assert task.payload["preflight_classification"] == "AUTO_SAFE"
        assert task.payload["selection_snapshot"] == final_payload["selection_snapshot"]
    await engine.dispose()
