"""Phase 2G.2 Inventory and notification regression contracts."""

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.cloud import CloudDiskInventory
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.transfer.cloud_inventory_service import CloudInventoryService
from app.transfer.episode_matcher import extract_video_episode_keys
from app.transfer.notifier import NotificationResult, TransferNotifier
from app.transfer.queue_service import TransferQueueService
from app.transfer.status import TransferOutcome, TransferStatus


@pytest.fixture()
async def sessions(tmp_path):
    from app.models import import_all_models

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/phase2g2.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    yield maker
    await engine.dispose()


@pytest.mark.parametrize(
    ("name", "season", "expected"),
    [
        ("S01E03-Gyy.mkv", 1, ("S01E03",)),
        ("episode E101.mkv", 1, ("S01E101",)),
        ("S01E03 预告.mkv", 1, ()),
        ("poster.jpg", 1, ()),
    ],
)
def test_inventory_parser_uses_regular_video_episode_rules(name, season, expected):
    assert extract_video_episode_keys(name, known_season=season) == expected


@pytest.mark.asyncio
async def test_verified_transfer_inventory_upsert_is_idempotent(sessions):
    async with sessions() as db, db.begin():
        first = await CloudInventoryService.upsert_verified_transfer(
            db,
            tmdb_id=322741,
            title="解垢",
            season=1,
            episode_key="S01E0003",
            file_name="S01E03-Gyy.mkv",
            verified=True,
            rel_path="S01/S01E03-Gyy.mkv",
            remote_file_id="remote-1",
            remote_folder_id="folder-1",
            provider="guangya",
            source="watchlist_scout",
        )
        second = await CloudInventoryService.upsert_verified_transfer(
            db,
            tmdb_id=322741,
            title="解垢",
            season=1,
            episode_key="S01E03",
            file_name="S01E03-Gyy.mkv",
            verified=True,
            rel_path="S01/S01E03-Gyy.mkv",
        )
        count = await db.scalar(select(func.count(CloudDiskInventory.id)))
        assert first.status == "INSERTED"
        assert second.status == "UNCHANGED"
        assert count == 1


@pytest.mark.asyncio
async def test_readback_failure_does_not_write_inventory(sessions):
    async with sessions() as db, db.begin():
        result = await CloudInventoryService.upsert_verified_transfer(
            db,
            tmdb_id=322741,
            title="解垢",
            season=1,
            episode_key="S01E03",
            file_name="S01E03-Gyy.mkv",
            verified=False,
        )
        count = await db.scalar(select(func.count(CloudDiskInventory.id)))
        assert result.status == "SKIPPED_UNVERIFIED"
        assert result.persisted is False
        assert count == 0


@pytest.mark.asyncio
async def test_scan_dry_run_is_zero_write_and_apply_converges(sessions):
    cloud_items = [
        {"fileId": "remote-1", "name": "S01E03-Gyy.mkv", "resType": 1},
        {"fileId": "poster-1", "name": "poster.jpg", "resType": 1},
    ]
    async with sessions() as db:
        plan = await CloudInventoryService.build_reconciliation_plan(
            db,
            tmdb_id=322741,
            season=1,
            title="解垢",
            cloud_items=cloud_items,
            provider="guangya",
            rel_path_prefix="S01",
        )
        assert plan.counts["to_insert"] == 1
        assert plan.counts["ignored_non_video"] == 1
        assert await db.scalar(select(func.count(CloudDiskInventory.id))) == 0

    async with sessions() as db, db.begin():
        applied = await CloudInventoryService.apply_reconciliation_plan(db, plan)
        assert [row.status for row in applied] == ["INSERTED"]

    async with sessions() as db:
        second_plan = await CloudInventoryService.build_reconciliation_plan(
            db,
            tmdb_id=322741,
            season=1,
            title="解垢",
            cloud_items=cloud_items,
            provider="guangya",
            rel_path_prefix="S01",
        )
        assert second_plan.counts["to_insert"] == 0
        assert second_plan.counts["to_update"] == 0
        assert second_plan.counts["unchanged"] == 1


@pytest.mark.asyncio
async def test_inventory_sync_failure_does_not_replay_or_fail_transfer(sessions, monkeypatch):
    from app.models.bot_settings import BotSettings
    from app.models.watchlist import SeriesWatchlist
    from app.transfer.orchestrator import TransferOrchestrator
    from app.transfer.queue_worker import TransferQueueWorker

    class Adapter:
        calls = 0

        async def transfer(self, payload):
            self.calls += 1
            return TransferOutcome(True, True, "folder", ("S01E01.mkv",))

    adapter = Adapter()

    async def fail_inventory(*args, **kwargs):
        raise RuntimeError("simulated inventory database failure")

    monkeypatch.setattr(CloudInventoryService, "upsert_verified_transfer", fail_inventory)
    async with sessions() as db, db.begin():
        db.add(BotSettings(key="global_pause", val="0"))
        db.add(BotSettings(key="transfer_paused", val="0"))
        db.add(SeriesWatchlist(tmdb_id=7, title="测试剧", season=1, status="FOLLOWING", collected_episodes=[]))
        resource = Resource(
            id=10,
            identity_key="7:dry-run:S01E01:inventory-test",
            tmdb_id=7,
            title="测试剧",
            season=1,
            episode=1,
            episode_key="S01E01",
            cloud_name="dry-run",
            share_url="https://example.invalid/share",
            source_type="watchlist_scout",
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

    worker = TransferQueueWorker(
        sessions,
        TransferOrchestrator({"dry-run": adapter}),
        worker_id="phase2g2-test",
        notifier=TransferNotifier(bot_token="", admin_tg_id=8586984520),
    )
    assert await worker.process_once() is True
    assert adapter.calls == 1
    async with sessions() as db:
        final_task = await db.get(TransferQueueTask, task.id)
        inventory_count = await db.scalar(select(func.count(CloudDiskInventory.id)))
        assert final_task.status == TransferStatus.COMPLETED
        assert final_task.result["inventory"]["status"] == "INVENTORY_SYNC_FAILED"
        assert inventory_count == 0


def test_test_message_resolver_ignores_source_role():
    notifier = TransferNotifier(
        bot_token="123456:FAKE_TOKEN",
        admin_tg_id=8586984520,
        default_channel_id="watchlist_scout",
    )
    target = notifier.resolve_notification_target({"source_channel_id": "framehdr"})
    assert target is not None
    assert target.chat_id == 8586984520
    assert target.source == "admin_tg_id"


@pytest.mark.asyncio
async def test_test_message_uses_resolver_and_records_message_id(monkeypatch):
    notifier = TransferNotifier(
        bot_token="123456:FAKE_TOKEN",
        admin_tg_id=8586984520,
        default_channel_id="watchlist_scout",
    )
    captured = {}

    async def fake_send(target, text, reply_markup=None):
        captured["target"] = target
        captured["text"] = text
        return NotificationResult(
            "SENT",
            True,
            target_chat_id=target.chat_id if target else None,
            target_source=target.source if target else None,
            telegram_message_id=123,
        )

    monkeypatch.setattr(notifier, "_send_telegram_result", fake_send)
    result = await notifier.send_test_message(
        task_payload={"source_channel_id": "framehdr"},
        text="测试 <消息>",
    )
    assert result.status == "SENT"
    assert result.telegram_message_id == 123
    assert captured["target"].source == "admin_tg_id"
    assert captured["text"] == "测试 &lt;消息&gt;"
