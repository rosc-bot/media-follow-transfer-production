import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.cloud import CloudDiskInventory
from app.transfer.cloud_inventory_service import CloudInventoryService


@pytest.mark.asyncio
async def test_promotion_inventory_keeps_canonical_media_and_category_prefix(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/inventory-promotion-category.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add(CloudDiskInventory(
            title="测试剧", clean_title="测试剧", tmdb_id=22, season=1, episode=1,
            file_name="测试剧.S01E01.mkv", rel_path="电视剧/国产剧/测试剧 {tmdbid-22}/S01/测试剧.S01E01.mkv",
        ))
        result = await CloudInventoryService.update_rel_paths_after_promotion(
            db,
            tmdb_id=22,
            series_folder_name="测试剧 {tmdbid-22}",
            destination_prefix="电视剧/国产剧/测试剧 {tmdbid-22}",
        )
        assert result["changed"] == 0
    await engine.dispose()


@pytest.mark.asyncio
async def test_promotion_inventory_path_is_flat_for_one_relevant_season(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/inventory-promotion-flat.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    expected = "电视剧/日番/单季剧 {tmdbid-223564}/单季剧.S01E01.mkv"
    async with sessions() as db, db.begin():
        db.add(CloudDiskInventory(
            title="单季剧", clean_title="单季剧", tmdb_id=223564, season=1, episode=1,
            file_name="单季剧.S01E01.mkv", rel_path="old/S01/单季剧.S01E01.mkv",
        ))
        result = await CloudInventoryService.update_rel_paths_after_promotion(
            db,
            tmdb_id=223564,
            series_folder_name="单季剧 {tmdbid-223564}",
            destination_prefix="电视剧/日番/单季剧 {tmdbid-223564}",
            relevant_seasons=[1],
        )
        row = await db.scalar(select(CloudDiskInventory).where(CloudDiskInventory.tmdb_id == 223564))
        assert result["changed"] == 1
        assert row.rel_path == expected
    await engine.dispose()
