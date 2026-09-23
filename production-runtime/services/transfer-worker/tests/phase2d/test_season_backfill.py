"""Phase 2D audit/backfill safety tests."""

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models import import_all_models

import_all_models()
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.season_backfill import apply_safe_backfill, build_audit_record


def _safe_record(*, task_id, resource_id, task_status="QUEUED", payload_season=None, resource_season=None):
    record = build_audit_record(
        task_id=task_id, resource_id=resource_id, tmdb_id=3, title="剧", episode_key="S01E07",
        payload_season=payload_season, resource_season=resource_season,
        watchlist_seasons={1}, candidate_seasons={1}, source_type="watchlist_scout",
    )
    record["task_status"] = task_status
    return record


def test_structured_episode_key_and_matching_watchlist_is_safe_infer():
    record = _safe_record(task_id=1, resource_id=2)
    assert record["confidence"] == "SAFE_INFER"
    assert record["inferred_season"] == 1


def test_multi_season_watchlist_is_not_conflicting_evidence_when_key_and_candidate_agree():
    record = build_audit_record(
        task_id=1, resource_id=2, tmdb_id=3, title="剧", episode_key="S02E04",
        payload_season=None, resource_season=None, watchlist_seasons={1, 2, 3}, candidate_seasons={2},
    )
    assert record["confidence"] == "SAFE_INFER"
    assert record["inferred_season"] == 2

    payload_conflict = _safe_record(task_id=1, resource_id=2, payload_season=2)
    assert payload_conflict["confidence"] == "NEEDS_REVIEW"
    candidate_conflict = build_audit_record(
        task_id=1, resource_id=2, tmdb_id=3, title="剧", episode_key="S01E07",
        payload_season=None, resource_season=None, watchlist_seasons={1}, candidate_seasons={2},
    )
    assert candidate_conflict["confidence"] == "NEEDS_REVIEW"


async def _task_fixture(tmp_path, *, status="QUEUED", season=None, payload=None):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        resource = Resource(identity_key="3:guangya:S01E07", tmdb_id=3, title="剧", media_type="tv",
                            season=season, episode=7, episode_key="S01E07", cloud_name="guangya",
                            share_url="https://example.test/a", source_type="watchlist_scout", status="READY")
        db.add(resource)
        await db.flush()
        task = TransferQueueTask(resource_id=resource.id, idempotency_key=f"task:{status}:{season}", status=status,
                                 payload=payload or {"resource_id": resource.id, "episode_keys": ["S01E07"]})
        db.add(task)
        await db.flush()
        return engine, sessions, resource.id, task.id


@pytest.mark.asyncio
async def test_apply_only_fills_blank_season_and_payload_without_touching_status(tmp_path):
    engine, sessions, resource_id, task_id = await _task_fixture(tmp_path)
    async with sessions() as db, db.begin():
        changed = await apply_safe_backfill(db, [_safe_record(task_id=task_id, resource_id=resource_id)])
        resource = await db.get(Resource, resource_id)
        task = await db.get(TransferQueueTask, task_id)
        assert changed == {"resource_season": 1, "task_payload_season": 1}
        assert resource.season == 1
        assert task.payload["season"] == 1
        assert task.status == "QUEUED"
        assert task.attempt_count == 0
    await engine.dispose()


@pytest.mark.asyncio
async def test_apply_never_overwrites_existing_season_or_payload(tmp_path):
    engine, sessions, resource_id, task_id = await _task_fixture(tmp_path, season=2, payload={"season": 2})
    async with sessions() as db, db.begin():
        changed = await apply_safe_backfill(db, [_safe_record(
            task_id=task_id, resource_id=resource_id, payload_season=2, resource_season=2,
        )])
        resource = await db.get(Resource, resource_id)
        task = await db.get(TransferQueueTask, task_id)
        assert changed == {"resource_season": 0, "task_payload_season": 0}
        assert resource.season == 2
        assert task.payload["season"] == 2
    await engine.dispose()


@pytest.mark.asyncio
async def test_apply_never_touches_failed_tasks_even_when_structured_evidence_is_safe(tmp_path):
    engine, sessions, resource_id, task_id = await _task_fixture(tmp_path, status="FAILED")
    async with sessions() as db, db.begin():
        record = _safe_record(task_id=task_id, resource_id=resource_id, task_status="FAILED")
        assert record["confidence"] == "SAFE_INFER"
        assert await apply_safe_backfill(db, [record]) == {"resource_season": 0, "task_payload_season": 0}
        resource = await db.get(Resource, resource_id)
        task = await db.get(TransferQueueTask, task_id)
        assert resource.season is None
        assert task.payload.get("season") is None
        assert task.status == "FAILED"
    await engine.dispose()
