"""Phase 2D audit query coverage."""

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models import import_all_models

import_all_models()
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.season_audit import audit_missing_seasons


@pytest.mark.asyncio
async def test_audit_reports_safe_task_and_does_not_mutate_db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        watch = SeriesWatchlist(tmdb_id=101, title="剧", season=1, status="FOLLOWING")
        resource = Resource(identity_key="101:x:S01E101", tmdb_id=101, title="剧", media_type="tv",
                            season=None, episode=101, episode_key="S01E101", cloud_name="guangya",
                            share_url="https://example.test/a", source_type="watchlist_scout")
        db.add_all([watch, resource])
        await db.flush()
        task = TransferQueueTask(resource_id=resource.id, idempotency_key="audit:1", status="QUEUED",
                                 payload={"resource_id": resource.id, "episode_keys": ["S01E101"]})
        db.add(task)
        await db.flush()
        report = await audit_missing_seasons(db)
        row = next(item for item in report["records"] if item["task_id"] == task.id)
        assert row["confidence"] == "SAFE_INFER"
        assert row["inferred_season"] == 1
        assert report["by_task_status"]["QUEUED"] == 1
        assert resource.season is None
        assert task.payload.get("season") is None
    await engine.dispose()


@pytest.mark.asyncio
async def test_audit_marks_unstructured_key_needs_review(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        resource = Resource(identity_key="101:x:E07", tmdb_id=101, title="剧", media_type="tv",
                            season=None, episode=7, episode_key="E07", cloud_name="guangya",
                            share_url="https://example.test/a", source_type="watchlist_scout")
        db.add(resource)
        await db.flush()
        task = TransferQueueTask(resource_id=resource.id, idempotency_key="audit:2", status="QUEUED",
                                 payload={"resource_id": resource.id, "episode_keys": ["E07"]})
        db.add(task)
        await db.flush()
        report = await audit_missing_seasons(db)
        row = next(item for item in report["records"] if item["task_id"] == task.id)
        assert row["confidence"] == "NEEDS_REVIEW"
        assert row["inferred_season"] is None
    await engine.dispose()
