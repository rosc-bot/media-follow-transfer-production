from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.transfer.errors import FileSelectionError
from app.transfer.queue_worker import TransferQueueWorker


@pytest.mark.asyncio
async def test_structural_identity_conflict_becomes_review_not_failed(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/structural-review.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add(Resource(
            id=31, identity_key="structural-review", tmdb_id=31, title="重复剧根",
            media_type="tv", season=2, episode=1, episode_key="S02E01", cloud_name="guangya",
            share_url="https://example/s/review", source_type="watchlist_scout",
        ))
        db.add(TransferQueueTask(
            id=31, resource_id=31, idempotency_key="structural-review-task",
            status="RUNNING", locked_by="worker", locked_at=datetime(2026, 9, 24, tzinfo=UTC),
            payload={"provider": "guangya", "tmdb_id": 31, "season": 2},
        ))
    worker = TransferQueueWorker(sessions, worker_id="test")

    result = await worker._record_failure(
        31,
        31,
        {"provider": "guangya", "tmdb_id": 31, "season": 2},
        FileSelectionError("DUPLICATE_SEASON_ROOT", "two S02 folders exist"),
    )

    async with sessions() as db:
        task = await db.get(TransferQueueTask, 31)
        assert result is False
        assert task.status == "PENDING"
        assert task.locked_at is None
        assert task.locked_by is None
        assert task.payload["preflight_classification"] == "NEEDS_REVIEW"
        assert task.payload["preflight_reason"] == "DUPLICATE_SEASON_ROOT"
        resource = await db.get(Resource, 31)
        assert resource.status != "FAILED"
    await engine.dispose()
