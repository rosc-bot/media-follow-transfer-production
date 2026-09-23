"""Phase 2E candidate state application is constrained to deterministic results."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models import import_all_models
from app.models.resource_candidate import ResourceCandidate

import_all_models()


@pytest.mark.asyncio
async def test_candidate_verification_updates_only_candidate_and_never_queue(tmp_path):
    from app.transfer.candidate_canary import apply_deterministic_candidate_status

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/candidate-state.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        candidate = ResourceCandidate(
            tmdb_id=10, title="剧", season=1, episode_key="S01E01", provider="guangya",
            share_url="https://pan.guangyapan.com/s/a", share_hash="a", status="DISCOVERED",
            discovered_at=datetime.now(UTC),
        )
        db.add(candidate)
    async with sessions() as db, db.begin():
        candidate = await db.scalar(select(ResourceCandidate))
        changed = await apply_deterministic_candidate_status(
            db, candidate=candidate, preflight_status="CANARY_SAFE", failure_code=None, failure_detail=""
        )
        assert changed is True
    async with sessions() as db:
        candidate = await db.scalar(select(ResourceCandidate))
        assert candidate.status == "VALIDATED"
    await engine.dispose()


@pytest.mark.asyncio
async def test_network_failure_does_not_mark_candidate_permanently_invalid(tmp_path):
    from app.transfer.candidate_canary import apply_deterministic_candidate_status

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/candidate-network.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        candidate = ResourceCandidate(
            tmdb_id=10, title="剧", season=1, episode_key="S01E01", provider="guangya",
            share_url="https://pan.guangyapan.com/s/a", share_hash="a", status="DISCOVERED",
            discovered_at=datetime.now(UTC),
        )
        db.add(candidate)
    async with sessions() as db, db.begin():
        candidate = await db.scalar(select(ResourceCandidate))
        await apply_deterministic_candidate_status(
            db, candidate=candidate, preflight_status="CANARY_REJECTED", failure_code="NETWORK_TIMEOUT", failure_detail="timeout"
        )
    async with sessions() as db:
        candidate = await db.scalar(select(ResourceCandidate))
        assert candidate.status == "TEMPORARY_FAILED"
    await engine.dispose()
