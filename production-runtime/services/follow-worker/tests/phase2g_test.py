"""Phase 2G production-closure regression contracts."""

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.follow.episode_keys import canonical_episode_key
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.transfer.candidate_service import mark_candidate_transferred
from app.transfer.notifier import TransferNotifier
from app.transfer.queue_service import TransferQueueService
from app.transfer.status import (
    SUCCESS_TERMINAL_STATUSES,
    TransferOutcome,
    TransferStatus,
    is_success_terminal,
)


@pytest.fixture()
async def sessions(tmp_path):
    from app.models import import_all_models

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/phase2g.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    yield maker
    await engine.dispose()


def test_success_and_completed_are_one_terminal_success_set():
    assert {str(value) for value in SUCCESS_TERMINAL_STATUSES} == {"SUCCESS", "COMPLETED"}
    assert is_success_terminal("SUCCESS")
    assert is_success_terminal("COMPLETED")
    assert not is_success_terminal("QUEUED")


def test_episode_keys_canonicalize_legacy_padding_and_large_episodes():
    assert canonical_episode_key(1, "S01E0001") == "S01E01"
    assert canonical_episode_key(1, "S1E1") == "S01E01"
    assert canonical_episode_key(1, "E2") == "S01E02"
    assert canonical_episode_key(1, 101) == "S01E101"


@pytest.mark.asyncio
async def test_mark_collected_writes_canonical_keys_and_collapses_padding(sessions):
    from app.follow.watchlist_service import WatchlistService

    async with sessions() as db, db.begin():
        db.add(SeriesWatchlist(
            tmdb_id=77,
            title="规范剧",
            season=1,
            status="FOLLOWING",
            collected_episodes=["S01E0001", "S01E01"],
        ))
        await db.flush()
        row = await WatchlistService.mark_collected(
            db,
            tmdb_id=77,
            season=1,
            episode_keys=["S01E0002", "S01E2", "S01E101"],
        )
        assert row is not None
        assert row.collected_episodes == ["S01E01", "S01E02", "S01E101"]


@pytest.mark.asyncio
async def test_completed_preflight_duplicate_uses_canonical_episode_identity(sessions):
    from tools.run_transfer_canary import run_preflight

    async with sessions() as db, db.begin():
        db.add(SeriesWatchlist(tmdb_id=322741, title="解垢", season=1, status="FOLLOWING", collected_episodes=[]))
        resource = Resource(
            id=11,
            identity_key="322741:guangya:S01E03:new",
            tmdb_id=322741,
            title="解垢",
            season=1,
            episode=3,
            episode_key="S01E03",
            cloud_name="guangya",
            share_url="https://example.invalid/share",
            source_type="watchlist_scout",
        )
        db.add(resource)
        await db.flush()
        task = await TransferQueueService.enqueue(
            db,
            resource_id=resource.id,
            provider="guangya",
            episode_keys=["S01E03"],
            payload={"tmdb_id": 322741, "season": 1, "episode_keys": ["S01E03"]},
        )
        historical = TransferQueueTask(
            resource_id=999,
            idempotency_key="historical-success-2",
            status="COMPLETED",
            payload={"tmdb_id": 322741, "episode_keys": ["S01E0003"]},
        )
        db.add(historical)
        task.status = "QUEUED"
    report = await run_preflight(task.id, session_factory=sessions)
    assert report["verdict"] == "CANARY_REJECTED"
    assert any(item["check"] == "no_duplicate_success" for item in report["rejections"])


@pytest.mark.asyncio
async def test_completed_task_blocks_new_same_episode_enqueue(sessions):
    async with sessions() as db, db.begin():
        completed = TransferQueueTask(
            resource_id=1,
            idempotency_key="historical-completed",
            status="COMPLETED",
            payload={"tmdb_id": 322741, "season": 1, "episode_keys": ["S01E0003"]},
        )
        db.add(completed)
        await db.flush()
        result = await TransferQueueService.enqueue_with_result(
            db,
            resource_id=2,
            provider="guangya",
            episode_keys=["S01E03"],
            payload={"tmdb_id": 322741, "season": 1, "episode_keys": ["S01E03"]},
        )
        assert result.task.id == completed.id
        assert result.created is False
        assert result.deduplicated is True


@pytest.mark.asyncio
async def test_success_task_blocks_duplicate_even_when_legacy_status_is_success(sessions):
    async with sessions() as db, db.begin():
        historical = TransferQueueTask(
            resource_id=1,
            idempotency_key="historical-success",
            status="SUCCESS",
            payload={"tmdb_id": 322741, "season": 1, "episode_keys": ["S01E03"]},
        )
        db.add(historical)
        await db.flush()
        result = await TransferQueueService.enqueue_with_result(
            db,
            resource_id=3,
            provider="guangya",
            episode_keys=["S01E03"],
            payload={"tmdb_id": 322741, "season": 1, "episode_keys": ["S01E03"]},
        )
        assert result.task.id == historical.id
        assert result.deduplicated is True


def test_notification_resolver_never_uses_source_role_as_chat_id():
    notifier = TransferNotifier(
        bot_token="123456:FAKE_TOKEN",
        admin_tg_id=8586984520,
        default_channel_id="-1004387965244",
    )
    target = notifier.resolve_notification_target({"source_channel_id": "framehdr"})
    assert target is not None
    assert target.chat_id == 8586984520
    assert target.source == "admin_tg_id"
    assert notifier.resolve_notification_target({"notification_chat_id": "framehdr"}) is not None


def test_notification_resolver_reports_missing_target_without_fallback():
    notifier = TransferNotifier(bot_token="123456:FAKE_TOKEN", admin_tg_id=None, default_channel_id="watchlist_scout")
    assert notifier.resolve_notification_target({"source_channel_id": "framehdr"}) is None


@pytest.mark.asyncio
async def test_notification_failure_does_not_change_transfer_success_or_verified_files(sessions):
    from app.models.bot_settings import BotSettings
    from app.transfer.orchestrator import TransferOrchestrator
    from app.transfer.queue_worker import TransferQueueWorker

    class Adapter:
        async def transfer(self, payload):
            return TransferOutcome(True, True, "folder", ("S01E01.mkv",))

    async with sessions() as db, db.begin():
        db.add(BotSettings(key="global_pause", val="0"))
        db.add(BotSettings(key="transfer_paused", val="0"))
        db.add(SeriesWatchlist(tmdb_id=7, title="测试剧", season=1, status="FOLLOWING", collected_episodes=[]))
        resource = Resource(
            id=10,
            identity_key="7:guangya:S01E01",
            tmdb_id=7,
            title="测试剧",
            season=1,
            episode=1,
            episode_key="S01E01",
            cloud_name="dry-run",
            share_url="https://example.invalid/share",
            source_type="watchlist_scout",
            file_names=[],
        )
        db.add(resource)
        await db.flush()
        task = await TransferQueueService.enqueue(
            db,
            resource_id=resource.id,
            provider="dry-run",
            episode_keys=["S01E01"],
            payload={"tmdb_id": 7, "title": "测试剧", "season": 1, "episode_keys": ["S01E01"]},
        )

    notifier = TransferNotifier(bot_token="", admin_tg_id=8586984520)
    worker = TransferQueueWorker(
        sessions,
        TransferOrchestrator({"dry-run": Adapter()}),
        worker_id="phase2g-test",
        notifier=notifier,
    )
    assert await worker.process_once() is True
    async with sessions() as db:
        final_task = await db.get(TransferQueueTask, task.id)
        final_resource = await db.get(Resource, resource.id)
        assert final_task.status == TransferStatus.COMPLETED
        assert final_task.result["verified"] is True
        assert final_task.result["notification"]["status"] == "NOTIFICATION_FAILED"
        assert final_resource.file_names == ["S01E01.mkv"]


@pytest.mark.asyncio
async def test_candidate_transferred_preserves_selection_attempt_semantics(sessions):
    from app.models.resource_candidate import ResourceCandidate

    async with sessions() as db, db.begin():
        candidate = ResourceCandidate(
            tmdb_id=7,
            title="测试剧",
            season=1,
            episode_key="S01E01",
            provider="guangya",
            share_url="https://example.invalid/share",
            share_hash="hash",
            status="SELECTED",
            attempt_count=1,
        )
        db.add(candidate)
        await db.flush()
        await mark_candidate_transferred(db, candidate=candidate)
        assert candidate.status == "TRANSFERRED"
        assert candidate.attempt_count == 1
        assert candidate.last_used_at is not None
