import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.bot_settings import BotSettings
from app.models.cloud import CloudConfig
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker


@pytest.mark.asyncio
async def test_hydration_builds_canonical_category_and_inventory_prefix_from_tmdb_metadata(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/canonical-hydration.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="0"),
            BotSettings(key="transfer_paused", val="1"),
            CloudConfig(name="guangya", auth_ref="auth", target_folder_id="completed", ongoing_target_folder_id="ongoing", enabled=True),
            Resource(
                id=1, identity_key="canonical-1", tmdb_id=324487, title="修复错误！", media_type="tv",
                year=2026, season=1, episode=2, episode_key="S01E02", share_url="https://example/s/1",
                cloud_name="guangya", source_type="watchlist_scout", file_names=["S01E02.mkv"],
            ),
        ])
        task = await TransferQueueService.enqueue(
            db, resource_id=1, provider="guangya", episode_keys=["S01E02"],
            payload={
                "tmdb_metadata": {
                    "id": 324487, "media_type": "tv", "origin_country": ["CN"], "original_language": "zh",
                    "genres": [{"id": 18, "name": "Drama"}], "metadata_complete": True,
                    "seasons": [{"season_number": 1}, {"season_number": 2}],
                },
            },
        )
    worker = TransferQueueWorker(sessions, worker_id="test")
    async with sessions() as db:
        row = await db.get(TransferQueueTask, task.id)
        payload = await worker._hydrate_payload(db, row)
    assert payload["media_category"] == "国产剧"
    assert payload["media_root"] == "电视剧"
    assert payload["ongoing_root_id"] == "ongoing"
    assert payload["completed_root_id"] == "completed"
    assert payload["inventory_prefix"].endswith("电视剧/国产剧/修复错误！ (2026) {tmdbid-324487}/S01")
    assert payload["archive_directory"].startswith("未完结追新 / 电视剧 / 国产剧")
    await engine.dispose()
