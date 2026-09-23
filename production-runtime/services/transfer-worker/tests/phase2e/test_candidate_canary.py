"""Phase 2E: candidate diagnostics never enqueue a transfer task."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models import import_all_models
from app.models.cloud import CloudConfig
from app.models.resource_candidate import ResourceCandidate
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist

import_all_models()


@pytest.mark.asyncio
async def test_candidate_listing_finds_safe_candidate_without_creating_queue(tmp_path):
    from app.transfer.candidate_canary import list_canary_resource_candidates

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/candidates.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add(CloudConfig(name="guangya", enabled=True, auth_ref="token", target_folder_id="dest"))
        db.add(SeriesWatchlist(tmdb_id=77, title="测试剧", season=1, status="FOLLOWING", collected_episodes=[]))
        db.add(ResourceCandidate(
            tmdb_id=77, title="测试剧", season=1, episode_key="S01E101", provider="guangya",
            share_url="https://pan.guangyapan.com/s/safe", share_hash="safe", source_type="watchlist_scout",
            status="VALIDATED", discovered_at=datetime.now(UTC),
        ))

    async def remote_ok(**_kwargs):
        return {
            "share_accessible": True, "has_video": True, "episode_match": True,
            "destination_auth": True, "destination_read": True,
        }

    result = await list_canary_resource_candidates(session_factory=sessions, remote_validator=remote_ok, hours=24)
    assert result["CANARY_RESOURCE_SAFE"] == 1
    row = result["rows"][0]
    assert row["candidate_id"]
    assert row["recommended_for_canary"] is True
    async with sessions() as db:
        assert (await db.execute(select(TransferQueueTask))).scalars().all() == []
        assert (await db.scalar(select(ResourceCandidate.status))) == "VALIDATED"
    await engine.dispose()
