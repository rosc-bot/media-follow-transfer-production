import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.follow.completion_promotion_service import CompletionPromotionService
from app.models.cloud import CloudConfig, CloudDiskInventory
from app.models.resource import Resource
from app.models.transfer import TransferJob, TransferQueueTask
from app.models.watchlist import SeriesWatchlist


async def _seed_complete_candidate(sessions, *, transfer_job_status=None, total_episodes=2):
    async with sessions() as db, db.begin():
        db.add(CloudConfig(name="guangya", auth_ref="test", enabled=True, target_folder_id="completed", ongoing_target_folder_id="ongoing"))
        db.add(SeriesWatchlist(
            tmdb_id=99001, title="严格完结剧", year=2025, media_type="tv", season=1,
            status="FOLLOWING", tmdb_series_status="Ended", total_episodes=2,
            last_aired_episode=2, collected_episodes=["S01E01", "S01E02"],
            remote_series_folder_id="ongoing-series", remote_destination_kind="ongoing",
        ))
        db.add(Resource(
            id=99001, identity_key="strict-promotion", tmdb_id=99001, title="严格完结剧", year=2025,
            media_type="tv", season=1, episode=2, episode_key="S01E02", cloud_name="guangya",
            share_url="https://pan.guangyapan.com/s/strict", source_type="watchlist_scout",
            file_names=["S01E01.mkv", "S01E02.mkv"],
        ))
        db.add_all([
            CloudDiskInventory(title="严格完结剧", clean_title="严格完结剧", tmdb_id=99001, season=1, episode=1, file_name="S01E01.mkv"),
            CloudDiskInventory(title="严格完结剧", clean_title="严格完结剧", tmdb_id=99001, season=1, episode=2, file_name="S01E02.mkv"),
        ])
        if transfer_job_status:
            db.add(TransferJob(
                id=99001, resource_id=99001, provider="guangya", status=transfer_job_status,
                target_folder_id="ongoing-series", expected_files=["S01E01.mkv", "S01E02.mkv"], result={},
            ))


@pytest.mark.asyncio
async def test_promotion_uses_fresh_tmdb_lifecycle_not_stale_ended_cache(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/tmdb-lifecycle.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await _seed_complete_candidate(sessions)

    async def fresh_metadata(_resource, *, force_refresh=False):
        assert force_refresh is True
        return {"id": 99001, "media_type": "tv", "status": "Returning Series", "seasons": [{"season_number": 1, "episode_count": 2}]}
    monkeypatch.setattr(CompletionPromotionService, "_tmdb_metadata", staticmethod(fresh_metadata))

    async with sessions() as db, db.begin():
        count = await CompletionPromotionService.enqueue_ready_promotions(
            db,
            physical_scan_results={99001: {
                "scan_status": "VERIFIED", "scan_timestamp": "2026-09-24T00:00:00+00:00",
                "scan_watermark": "physical:99001:test", "cloud_episode_keys_by_season": {1: {"S01E01", "S01E02"}},
            }},
        )
    assert count == 0
    async with sessions() as db:
        tasks = list((await db.scalars(select(TransferQueueTask))).all())
        assert tasks == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_promotion_uses_tmdb_season_episode_count_not_stale_database_total(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/tmdb-episode-count.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await _seed_complete_candidate(sessions, total_episodes=2)

    async def fresh_metadata(_resource, *, force_refresh=False):
        return {"id": 99001, "media_type": "tv", "status": "Ended", "seasons": [{"season_number": 1, "episode_count": 3}]}
    monkeypatch.setattr(CompletionPromotionService, "_tmdb_metadata", staticmethod(fresh_metadata))

    async with sessions() as db, db.begin():
        count = await CompletionPromotionService.enqueue_ready_promotions(
            db,
            physical_scan_results={99001: {
                "scan_status": "VERIFIED", "scan_timestamp": "2026-09-24T00:00:00+00:00",
                "scan_watermark": "physical:99001:test", "cloud_episode_keys_by_season": {1: {"S01E01", "S01E02"}},
            }},
        )
    assert count == 0
    async with sessions() as db:
        tasks = list((await db.scalars(select(TransferQueueTask))).all())
        assert tasks == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_running_transfer_job_blocks_promotion_even_without_active_queue_task(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/running-transfer-job.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await _seed_complete_candidate(sessions, transfer_job_status="RUNNING")

    async def fresh_metadata(_resource, *, force_refresh=False):
        return {"id": 99001, "media_type": "tv", "status": "Ended", "seasons": [{"season_number": 1, "episode_count": 2}]}
    monkeypatch.setattr(CompletionPromotionService, "_tmdb_metadata", staticmethod(fresh_metadata))

    async with sessions() as db, db.begin():
        count = await CompletionPromotionService.enqueue_ready_promotions(
            db,
            physical_scan_results={99001: {
                "scan_status": "VERIFIED", "scan_timestamp": "2026-09-24T00:00:00+00:00",
                "scan_watermark": "physical:99001:test", "cloud_episode_keys_by_season": {1: {"S01E01", "S01E02"}},
            }},
        )
    assert count == 0
    async with sessions() as db:
        tasks = list((await db.scalars(select(TransferQueueTask))).all())
        assert tasks == []
    await engine.dispose()
