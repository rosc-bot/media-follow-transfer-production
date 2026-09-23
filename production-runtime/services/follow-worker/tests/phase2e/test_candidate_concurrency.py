"""Phase 2E: candidate listing performs bounded concurrent read probes."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models import import_all_models
from app.models.cloud import CloudConfig
from app.models.resource_candidate import ResourceCandidate
from app.models.watchlist import SeriesWatchlist

import_all_models()


@pytest.mark.asyncio
async def test_candidate_listing_uses_bounded_concurrency(tmp_path):
    from app.transfer.candidate_canary import list_canary_resource_candidates

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/bounded.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add(CloudConfig(name="guangya", enabled=True, auth_ref="token", target_folder_id="dest"))
        for i in range(8):
            db.add(SeriesWatchlist(tmdb_id=100+i, title=f"剧{i}", season=1, status="FOLLOWING", collected_episodes=[]))
            db.add(ResourceCandidate(
                tmdb_id=100+i, title=f"剧{i}", season=1, episode_key="S01E01", provider="guangya",
                share_url=f"https://pan.guangyapan.com/s/{i}", share_hash=str(i), source_type="watchlist_scout",
                status="DISCOVERED", discovered_at=datetime.now(UTC)-timedelta(minutes=i),
            ))

    active = 0
    peak = 0
    async def remote_ok(**_kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return {"share_accessible": True, "has_video": True, "episode_match": True, "destination_auth": True, "destination_read": True}

    result = await list_canary_resource_candidates(session_factory=sessions, remote_validator=remote_ok, hours=24, limit=8, concurrency=3)
    assert result["CANARY_RESOURCE_SAFE"] == 8
    assert peak <= 3
    await engine.dispose()
