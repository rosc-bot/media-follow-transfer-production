import pytest

from app.transfer.missing_episode_preflight import (
    AUTO_SAFE,
    NEEDS_REVIEW,
    plan_missing_episode_transfer,
)


def _share_files():
    return [
        {"fileId": "e01", "name": "Show.S01E01.mkv"},
        {"fileId": "e02", "name": "Show.S01E02.mkv"},
        {"fileId": "e03", "name": "Show.S01E03.mkv"},
    ]


def test_cloud_present_with_missing_inventory_is_not_restored_and_is_reconciled():
    plan = plan_missing_episode_transfer(
        _share_files(),
        season=1,
        trigger_episode_keys=["S01E01"],
        collected_episode_keys=["S01E01"],
        inventory_episode_keys=[],
        cloud_episode_keys=["S01E01"],
        cloud_scan_verified=True,
    )

    assert plan.classification == AUTO_SAFE
    assert plan.missing_episode_keys == ("S01E02", "S01E03")
    assert plan.presence_decisions["S01E01"]["classification"] == "PRESENT_CONFIRMED"
    assert plan.metadata_reconcile == {"S01E01": ("inventory",)}


def test_pending_is_review_only_not_an_execution_active_status():
    from app.transfer.status import EXECUTION_ACTIVE_STATUSES, REVIEW_STATUS

    assert EXECUTION_ACTIVE_STATUSES == frozenset({"QUEUED", "RUNNING", "RETRY_WAIT"})
    assert REVIEW_STATUS == "PENDING"
    assert "PENDING" not in EXECUTION_ACTIVE_STATUSES


@pytest.mark.asyncio
async def test_switch_resource_groups_same_share_episodes_into_one_batch_task(tmp_path):
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from app.transfer.candidate_service import record_candidate
    from app.transfer.switch_resource import switch_resource

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/switch-batch.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        source = Resource(
            identity_key="991:guangya:S01E08",
            tmdb_id=991,
            title="切源批量剧",
            media_type="tv",
            season=1,
            episode=8,
            episode_key="S01E08",
            cloud_name="guangya",
            share_url="https://pan.guangyapan.com/s/failed",
            source_type="watchlist_scout",
        )
        db.add(source)
        await db.flush()
        failed = TransferQueueTask(
            task_type="TRANSFER",
            resource_id=source.id,
            idempotency_key="failed-batch",
            status="FAILED",
            payload={
                "tmdb_id": 991,
                "title": "切源批量剧",
                "season": 1,
                "episode_keys": ["S01E08", "S01E09"],
                "share_url": source.share_url,
            },
        )
        db.add(failed)
        for key in ("S01E08", "S01E09"):
            await record_candidate(
                db,
                tmdb_id=991,
                title="切源批量剧",
                season=1,
                episode_key=key,
                provider="guangya",
                share_url="https://pan.guangyapan.com/s/replacement-pack",
            )
        await db.flush()

        result = await switch_resource(
            db,
            task_id=failed.id,
            resource_db_path=str(tmp_path / "empty-resource-db.sqlite"),
        )
        tasks = list((await db.scalars(
            select(TransferQueueTask).where(TransferQueueTask.id.in_([row["task_id"] for row in result["new_tasks"]]))
        )).all())
        resources_created = await db.scalar(
            select(func.count(Resource.id)).where(Resource.tmdb_id == 991)
        )

        assert len(result["new_tasks"]) == 1
        assert len(tasks) == 1
        assert tasks[0].payload["episode_keys"] == ["S01E08", "S01E09"]
        assert tasks[0].payload["selection_mode"] == "MISSING_EPISODES"
        assert resources_created == 2

    await engine.dispose()


def test_success_card_separates_verified_cumulative_progress_from_this_task():
    from app.transfer.notifier import build_success_card

    card = build_success_card(
        task_payload={
            "title": "通知计数剧",
            "media_type": "tv",
            "season": 1,
            "episode_keys": ["S01E03"],
            "selected_file_names": ["通知计数剧.S01E03.mkv"],
            "collected_episode_keys": ["S01E01", "S01E02", "S01E03"],
            "inventory_episode_keys": ["S01E01", "S01E02", "S01E03"],
            "cloud_verified_episode_keys": ["S01E01", "S01E02", "S01E03"],
            "collection_progress_verified": True,
            "total_episodes": 8,
            "missing_episode_keys": ["S01E04", "S01E05", "S01E06", "S01E07", "S01E08"],
        },
        transfer_result={
            "verified": True,
            "selected_file_names": ["通知计数剧.S01E03.mkv"],
            "selected_episode_keys": ["S01E03"],
            "verified_episode_files": [{"episode_key": "S01E03"}],
        },
    )

    text = card.caption.replace("<b>", "").replace("</b>", "")
    assert "收录进度：S01 已收录 3 / 8" in text
    assert "本次新增：S01E03" in text
    assert "当前缺集：S01E04-E08" in text
    assert "本次已核验 1 个文件" in text
    assert "收录集数：S01E03（本次 1 集，共 8 集）" not in text


@pytest.mark.parametrize(
    ("collected", "inventory", "cloud", "verified", "truncated", "pagination_complete", "completed", "classification", "reason", "reconcile"),
    [
        (True, False, True, True, False, True, False, AUTO_SAFE, "MISSING_EPISODES_CONFIRMED_BY_VERIFIED_CLOUD", ("inventory",)),
        (False, True, True, True, False, True, False, AUTO_SAFE, "MISSING_EPISODES_CONFIRMED_BY_VERIFIED_CLOUD", ("collected",)),
        (False, False, True, True, False, True, False, AUTO_SAFE, "MISSING_EPISODES_CONFIRMED_BY_VERIFIED_CLOUD", ("collected", "inventory")),
        (False, False, False, True, False, True, False, AUTO_SAFE, "MISSING_EPISODES_CONFIRMED_BY_VERIFIED_CLOUD", ()),
        (True, False, False, True, False, True, False, NEEDS_REVIEW, "LEDGER_CONFLICT:S01E01", ()),
        (False, True, False, True, False, True, False, NEEDS_REVIEW, "LEDGER_CONFLICT:S01E01", ()),
        (False, False, False, False, False, True, False, NEEDS_REVIEW, "CLOUD_UNVERIFIED", ()),
        (False, False, False, True, True, True, False, NEEDS_REVIEW, "CLOUD_UNVERIFIED", ()),
        (False, False, False, True, False, False, False, NEEDS_REVIEW, "CLOUD_UNVERIFIED", ()),
        (False, False, False, True, False, True, True, NEEDS_REVIEW, "COMPLETED_TASK_WITHOUT_CLOUD_CLOSURE:S01E01", ()),
    ],
)
def test_presence_planner_uses_verified_cloud_as_physical_truth(
    collected, inventory, cloud, verified, truncated, pagination_complete,
    completed, classification, reason, reconcile,
):
    plan = plan_missing_episode_transfer(
        _share_files(),
        season=1,
        trigger_episode_keys=["S01E01"],
        collected_episode_keys=["S01E01"] if collected else [],
        inventory_episode_keys=[1] if inventory else [],
        cloud_episode_keys=["S01E01"] if cloud else [],
        completed_episode_keys=["S01E01"] if completed else [],
        cloud_scan_verified=verified,
        cloud_scan_truncated=truncated,
        cloud_pagination_complete=pagination_complete,
    )

    assert plan.classification == classification
    assert plan.reason == reason
    if cloud and verified and not truncated and pagination_complete:
        assert plan.presence_decisions["S01E01"]["classification"] == "PRESENT_CONFIRMED"
        assert plan.metadata_reconcile.get("S01E01", ()) == reconcile
        assert "S01E01" not in plan.missing_episode_keys
    if not cloud and verified and not truncated and pagination_complete and not collected and not inventory and not completed:
        assert plan.presence_decisions["S01E01"]["classification"] == "MISSING_CONFIRMED"
        assert "S01E01" in plan.missing_episode_keys


@pytest.mark.asyncio
async def test_multi_episode_success_sends_one_notification_message():
    from unittest.mock import AsyncMock, patch

    from app.transfer.notifier import TransferNotifier

    notifier = TransferNotifier(bot_token="123456:TEST_TOKEN", success_chat="@guangyazhauncun")
    response = __import__("httpx").Response(
        200,
        json={"ok": True, "result": {"message_id": 1}},
        request=__import__("httpx").Request("POST", "https://example.invalid"),
    )
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=response) as send:
        result = await notifier.notify_success_result(
            task_payload={
                "title": "批量通知剧",
                "season": 1,
                "episode_keys": ["S01E08", "S01E09", "S01E10", "S01E11", "S01E12"],
                "selected_episode_keys": ["S01E08", "S01E09", "S01E10", "S01E11", "S01E12"],
                "selected_file_names": [f"S01E{episode:02d}.mkv" for episode in range(8, 13)],
                "collection_progress_verified": False,
            },
            transfer_result={"verified": True, "selected_file_names": [f"S01E{episode:02d}.mkv" for episode in range(8, 13)]},
        )

    assert result.sent is True
    assert send.call_count == 1


@pytest.mark.asyncio
async def test_batch_preflight_exception_persists_real_message_stage_and_releases_lock(tmp_path, caplog):
    import logging
    from datetime import UTC, datetime

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from app.transfer.queue_worker import TransferQueueWorker

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/batch-exception.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    payload = {"episode_keys": ["S01E08", "S01E09"]}
    async with sessions() as db, db.begin():
        db.add(Resource(
            id=901, identity_key="901:guangya:S01E08", tmdb_id=901, title="预检异常剧",
            media_type="tv", season=1, episode=8, episode_key="S01E08",
            share_url="https://example.invalid/share", source_type="watchlist_scout",
        ))
        db.add(TransferQueueTask(
            id=901, task_type="TRANSFER", resource_id=901, idempotency_key="batch-error-901",
            status="RUNNING", locked_by="test-worker", locked_at=datetime.now(UTC), payload=payload,
        ))
    worker = TransferQueueWorker(sessions, worker_id="test-worker")

    async def fail_batch(_db, *, task, resource, payload):
        assert payload["selection_mode"] == "MISSING_EPISODES"
        payload["preflight_stage"] = "SHARE_PROBE"
        raise RuntimeError("share listing exploded with useful detail")

    worker._batch_episode_presence_preflight = fail_batch
    with caplog.at_level(logging.ERROR):
        result = await worker._runtime_final_preflight(task_id=901, resource_id=901, payload=dict(payload))

    assert result is None
    async with sessions() as db:
        task = await db.get(TransferQueueTask, 901)
        assert task.status == "PENDING"
        assert task.locked_at is None and task.locked_by is None
        assert task.payload["preflight_reason"] == "BATCH_PREFLIGHT_EXCEPTION"
        assert task.payload["preflight_stage"] == "SHARE_PROBE"
        assert "RuntimeError: share listing exploded with useful detail" in task.payload["batch_presence_preflight"]["detail"]
    assert "stage=SHARE_PROBE" in caplog.text
    assert "share listing exploded with useful detail" in caplog.text
    assert "Traceback (most recent call last)" in caplog.text
    await engine.dispose()


@pytest.mark.asyncio
async def test_batch_preflight_timeout_is_bounded_and_releases_task_lock(tmp_path, monkeypatch):
    import time
    from datetime import UTC, datetime

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from app.transfer.queue_worker import TransferQueueWorker

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/batch-timeout.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    payload = {"selection_mode": "MISSING_EPISODES", "episode_keys": ["S01E08"]}
    async with sessions() as db, db.begin():
        db.add(Resource(
            id=902, identity_key="902:guangya:S01E08", tmdb_id=902, title="预检超时剧",
            media_type="tv", season=1, episode=8, episode_key="S01E08",
            share_url="https://example.invalid/share", source_type="watchlist_scout",
        ))
        db.add(TransferQueueTask(
            id=902, task_type="TRANSFER", resource_id=902, idempotency_key="batch-timeout-902",
            status="RUNNING", locked_by="test-worker", locked_at=datetime.now(UTC), payload=payload,
        ))
    worker = TransferQueueWorker(sessions, worker_id="test-worker")

    async def slow_batch(_db, *, task, resource, payload):
        payload["preflight_stage"] = "CLOUD_PRESENCE_SCAN"
        await __import__("asyncio").sleep(10)

    worker._batch_episode_presence_preflight = slow_batch
    monkeypatch.setattr("app.transfer.queue_worker.BATCH_PREFLIGHT_TIMEOUT_SECONDS", 0.05)
    started = time.monotonic()
    result = await worker._runtime_final_preflight(task_id=902, resource_id=902, payload=dict(payload))
    elapsed = time.monotonic() - started

    assert result is None
    assert elapsed < 1
    async with sessions() as db:
        task = await db.get(TransferQueueTask, 902)
        assert task.status == "PENDING"
        assert task.locked_at is None and task.locked_by is None
        assert task.payload["preflight_reason"] == "BATCH_PREFLIGHT_TIMEOUT"
        assert task.payload["preflight_stage"] == "CLOUD_PRESENCE_SCAN"
    await engine.dispose()


@pytest.mark.asyncio
async def test_verified_cloud_metadata_reconcile_repairs_inventory_and_collected(tmp_path):
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.cloud import CloudDiskInventory
    from app.models.watchlist import SeriesWatchlist
    from app.transfer.queue_worker import TransferQueueWorker

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/reconcile.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        watchlist = SeriesWatchlist(
            tmdb_id=903, title="云盘账本剧", season=1, status="FOLLOWING", collected_episodes=[]
        )
        db.add(watchlist)
        await db.flush()
        watchlist_id = watchlist.id
    worker = TransferQueueWorker(sessions)

    result = await worker._reconcile_verified_cloud_presence(
        tmdb_id=903,
        season=1,
        title="云盘账本剧",
        watchlist_id=watchlist_id,
        series_root_id="verified-series-root",
        inventory_prefix="电视剧/国产剧/云盘账本剧 {tmdbid-903}",
        metadata_reconcile={"S01E04": ("collected", "inventory")},
        cloud_files=[{
            "episode_key": "S01E04",
            "name": "云盘账本剧.S01E04.mkv",
            "file_id": "verified-file-4",
            "path": "S01/云盘账本剧.S01E04.mkv",
        }],
        provider="guangya",
    )

    assert result == {"inventory_added": 1, "inventory_updated": 0, "collected_added": 1}
    async with sessions() as db:
        watchlist = await db.get(SeriesWatchlist, watchlist_id)
        inventory = list((await db.scalars(select(CloudDiskInventory).where(
            CloudDiskInventory.tmdb_id == 903,
            CloudDiskInventory.season == 1,
            CloudDiskInventory.episode == 4,
        ))).all())
        assert watchlist.collected_episodes == ["S01E04"]
        assert len(inventory) == 1
        assert inventory[0].file_name == "云盘账本剧.S01E04.mkv"
        assert inventory[0].rel_path.endswith("S01/云盘账本剧.S01E04.mkv")
    await engine.dispose()


@pytest.mark.asyncio
async def test_switch_resource_does_not_reuse_pending_review_task(tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from app.transfer.candidate_service import record_candidate
    from app.transfer.switch_resource import switch_resource

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/pending-switch.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        failed_resource = Resource(
            identity_key="904:old-source", tmdb_id=904, title="换源不复用PENDING剧",
            media_type="tv", season=1, episode=8, episode_key="S01E08",
            cloud_name="guangya", share_url="https://pan.guangyapan.com/s/failed",
            source_type="watchlist_scout",
        )
        pending_resource = Resource(
            identity_key="904:legacy-review", tmdb_id=904, title="换源不复用PENDING剧",
            media_type="tv", season=1, episode=8, episode_key="S01E08",
            cloud_name="guangya", share_url="https://pan.guangyapan.com/s/new-candidate",
            source_type="watchlist_scout",
        )
        db.add_all([failed_resource, pending_resource])
        await db.flush()
        failed = TransferQueueTask(
            task_type="TRANSFER", resource_id=failed_resource.id,
            idempotency_key="source-failed-904", status="FAILED",
            payload={"tmdb_id": 904, "title": "换源不复用PENDING剧", "season": 1,
                    "episode_keys": ["S01E08"], "share_url": failed_resource.share_url},
        )
        pending = TransferQueueTask(
            task_type="TRANSFER", resource_id=pending_resource.id,
            idempotency_key="old-pending-904", status="PENDING",
            payload={"tmdb_id": 904, "title": "换源不复用PENDING剧", "season": 1,
                    "episode_keys": ["S01E08"], "share_url": pending_resource.share_url},
        )
        db.add_all([failed, pending])
        await record_candidate(
            db,
            tmdb_id=904,
            title="换源不复用PENDING剧",
            season=1,
            episode_key="S01E08",
            provider="guangya",
            share_url=pending_resource.share_url,
        )
        await db.flush()

        result = await switch_resource(
            db,
            task_id=failed.id,
            resource_db_path=str(tmp_path / "empty-candidates.sqlite"),
        )
        assert len(result["new_tasks"]) == 1
        assert result["new_tasks"][0]["task_id"] != pending.id
        new_task = await db.get(TransferQueueTask, result["new_tasks"][0]["task_id"])
        assert new_task.status == "QUEUED"
        assert new_task.payload["episode_keys"] == ["S01E08"]
        assert (await db.get(TransferQueueTask, pending.id)).status == "PENDING"
    await engine.dispose()


def test_promotion_active_gate_ignores_pending_review_rows():
    from app.follow.completion_promotion_service import _ACTIVE_TRANSFER_STATUSES

    assert _ACTIVE_TRANSFER_STATUSES == frozenset({"QUEUED", "RUNNING", "RETRY_WAIT"})
    assert "PENDING" not in _ACTIVE_TRANSFER_STATUSES


@pytest.mark.asyncio
async def test_stale_pending_requeues_only_after_verified_auto_safe_candidate(tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from app.transfer.candidate_service import record_candidate
    from app.transfer.queue_worker import TransferQueueWorker

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/stale-pending.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        old_resource = Resource(
            id=905, identity_key="905:old-share", tmdb_id=905, title="待审恢复剧",
            media_type="tv", season=1, episode=8, episode_key="S01E08",
            cloud_name="guangya", share_url="https://pan.guangyapan.com/s/old",
            source_type="watchlist_scout",
        )
        new_resource = Resource(
            id=906, identity_key="905:new-share", tmdb_id=905, title="待审恢复剧",
            media_type="tv", season=1, episode=8, episode_key="S01E08",
            cloud_name="guangya", share_url="https://pan.guangyapan.com/s/new",
            source_type="watchlist_scout",
        )
        db.add_all([old_resource, new_resource])
        await db.flush()
        stale = TransferQueueTask(
            id=905, task_type="TRANSFER", resource_id=old_resource.id,
            idempotency_key="stale-pending-905", status="PENDING",
            payload={"tmdb_id": 905, "season": 1, "episode_keys": ["S01E08", "S01E09"],
                    "share_url": old_resource.share_url},
        )
        current = TransferQueueTask(
            id=906, task_type="TRANSFER", resource_id=new_resource.id,
            idempotency_key="new-candidate-906", status="RUNNING", locked_by="worker",
            payload={"tmdb_id": 905, "season": 1, "episode_keys": ["S01E08", "S01E09"],
                    "share_url": new_resource.share_url},
        )
        db.add_all([stale, current])
        for episode_key in ("S01E08", "S01E09"):
            await record_candidate(
                db, tmdb_id=905, title="待审恢复剧", season=1, episode_key=episode_key,
                provider="guangya", share_url=new_resource.share_url,
            )
        await db.flush()
        worker = TransferQueueWorker(sessions, worker_id="worker")
        payload = {
            "share_url": new_resource.share_url,
            "episode_keys": ["S01E08", "S01E09"],
            "batch_presence_preflight": {
                "cloud_scan_verified": True,
                "presence_decisions": {
                    "S01E08": {"classification": "MISSING_CONFIRMED"},
                    "S01E09": {"classification": "MISSING_CONFIRMED"},
                },
            },
        }
        result = await worker._recover_stale_pending_after_final_preflight(
            db,
            current_task=current,
            resource=new_resource,
            payload=payload,
            missing_episode_keys=["S01E08", "S01E09"],
        )

        assert result["queued_reactivated"] == 1
        assert result["stale_pending_recovered"] == 1
        assert stale.status == "QUEUED"
        assert stale.resource_id == new_resource.id
        assert stale.payload["episode_keys"] == ["S01E08", "S01E09"]
        assert stale.payload["selection_mode"] == "MISSING_EPISODES"
        assert stale.payload["candidate_switched"] is True
        assert current.status == "CANCELLED"
    await engine.dispose()


@pytest.mark.asyncio
async def test_stale_pending_cloud_present_is_closed_without_requeue(tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from app.transfer.queue_worker import TransferQueueWorker

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/pending-cloud-closure.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        resource = Resource(
            id=909, identity_key="909:new-source", tmdb_id=909, title="云端旧任务剧",
            media_type="tv", season=1, episode=10, episode_key="S01E10",
            cloud_name="guangya", share_url="https://pan.guangyapan.com/s/new-source",
            source_type="watchlist_scout",
        )
        old_resource = Resource(
            id=910, identity_key="909:old-source", tmdb_id=909, title="云端旧任务剧",
            media_type="tv", season=1, episode=8, episode_key="S01E08",
            cloud_name="guangya", share_url="https://pan.guangyapan.com/s/old-source",
            source_type="watchlist_scout",
        )
        db.add_all([resource, old_resource])
        await db.flush()
        stale = TransferQueueTask(
            id=910, task_type="TRANSFER", resource_id=old_resource.id,
            idempotency_key="stale-cloud-910", status="PENDING",
            payload={"tmdb_id": 909, "season": 1, "episode_keys": ["S01E08", "S01E09"],
                    "share_url": old_resource.share_url},
        )
        current = TransferQueueTask(
            id=909, task_type="TRANSFER", resource_id=resource.id,
            idempotency_key="new-cloud-task-909", status="RUNNING", locked_by="worker",
            payload={"tmdb_id": 909, "season": 1, "episode_keys": ["S01E10"],
                    "share_url": resource.share_url},
        )
        db.add_all([stale, current])
        await db.flush()
        worker = TransferQueueWorker(sessions, worker_id="worker")
        result = await worker._recover_stale_pending_after_final_preflight(
            db,
            current_task=current,
            resource=resource,
            payload={
                "share_url": resource.share_url,
                "batch_presence_preflight": {
                    "cloud_scan_verified": True,
                    "presence_decisions": {
                        "S01E08": {"classification": "PRESENT_CONFIRMED"},
                        "S01E09": {"classification": "PRESENT_CONFIRMED"},
                        "S01E10": {"classification": "MISSING_CONFIRMED"},
                    },
                },
            },
            missing_episode_keys=["S01E10"],
        )

        assert result["queued_reactivated"] == 0
        assert result["stale_pending_recovered"] == 1
        assert stale.status == "PENDING"
        assert stale.error_message.endswith("RECONCILED_ALREADY_IN_CLOUD")
        assert current.status == "RUNNING"
    await engine.dispose()


@pytest.mark.asyncio
async def test_readonly_batch_diagnostic_does_not_mutate_queue_task(tmp_path, monkeypatch):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from tools.inspect_batch_preflight import inspect_batch_preflight

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/readonly-diagnostic.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    payload = {"selection_mode": "MISSING_EPISODES", "episode_keys": ["S01E03", "S01E04"]}
    async with sessions() as db, db.begin():
        db.add(Resource(
            id=911, identity_key="911:readonly", tmdb_id=911, title="只读诊断剧",
            media_type="tv", season=1, episode=3, episode_key="S01E03",
            share_url="https://pan.guangyapan.com/s/readonly", source_type="watchlist_scout",
        ))
        db.add(TransferQueueTask(
            id=911, task_type="TRANSFER", resource_id=911, idempotency_key="readonly-task-911",
            status="PENDING", locked_at=None, locked_by=None, error_message="keep-this-review",
            payload=payload,
        ))

    async def fake_hydrate(_self, _db, task):
        return dict(task.payload)

    async def fake_batch(_self, _db, *, task, resource, payload):
        payload["preflight_stage"] = "CLOUD_PRESENCE_SCAN"
        return {
            "classification": NEEDS_REVIEW,
            "reason": "CLOUD_UNVERIFIED",
            "preflight_stage": "CLOUD_PRESENCE_SCAN",
            "share_episode_keys": ["S01E03", "S01E04"],
            "missing_episode_keys": [],
            "_share_evidence": {"share_readable": True},
            "_cloud_scan_verified": False,
        }

    monkeypatch.setattr("app.transfer.queue_worker.TransferQueueWorker._hydrate_payload", fake_hydrate)
    monkeypatch.setattr("app.transfer.queue_worker.TransferQueueWorker._batch_episode_presence_preflight", fake_batch)
    report = await inspect_batch_preflight(911, session_factory=sessions)

    assert report["read_only"] is True
    assert report["classification"] == NEEDS_REVIEW
    assert report["reason"] == "CLOUD_UNVERIFIED"
    async with sessions() as db:
        task = await db.get(TransferQueueTask, 911)
        assert task.status == "PENDING"
        assert task.error_message == "keep-this-review"
        assert task.locked_at is None and task.locked_by is None
    await engine.dispose()


@pytest.mark.asyncio
async def test_worker_infers_batch_mode_before_local_ledger_gates(tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.bot_settings import BotSettings
    from app.models.cloud import CloudConfig, CloudDiskInventory
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from app.models.watchlist import SeriesWatchlist
    from app.transfer.queue_worker import TransferQueueWorker

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/infer-batch.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="0"),
            BotSettings(key="transfer_paused", val="0"),
            CloudConfig(
                name="guangya", enabled=True, auth_ref="opaque-auth-ref",
                target_folder_id="root", ongoing_target_folder_id="ongoing",
            ),
            SeriesWatchlist(
                tmdb_id=907, title="旧批量任务剧", season=1, status="FOLLOWING",
                collected_episodes=["S01E08"],
            ),
            CloudDiskInventory(
                title="旧批量任务剧", clean_title="旧批量任务剧", tmdb_id=907,
                season=1, episode=8, file_name="旧批量任务剧.S01E08.mkv",
            ),
            Resource(
                id=907, identity_key="907:old-batch", tmdb_id=907, title="旧批量任务剧",
                media_type="tv", season=1, episode=8, episode_key="S01E08",
                cloud_name="guangya", share_url="https://pan.guangyapan.com/s/old-batch",
                source_type="watchlist_scout", file_names=["旧批量任务剧.S01E08.mkv"],
            ),
        ])
        db.add(TransferQueueTask(
            id=907, task_type="TRANSFER", resource_id=907, idempotency_key="old-batch-task-907",
            status="QUEUED",
            payload={
                "resource_id": 907,
                "share_url": "https://pan.guangyapan.com/s/old-batch",
                "tmdb_id": 907,
                "season": 1,
                "episode_keys": ["S01E08", "S01E09"],
                "title": "旧批量任务剧",
                "tmdb_metadata": {
                    "id": 907,
                    "media_type": "tv",
                    "origin_country": ["CN"],
                    "original_language": "zh",
                    "genres": [{"id": 18, "name": "Drama"}],
                    "metadata_complete": True,
                    "seasons": [{"season_number": 1}],
                },
            },
        ))
    worker = TransferQueueWorker(sessions, worker_id="batch-inference-test")

    claimed = await worker._claim_payload()

    assert claimed is not None
    task_id, _, payload = claimed
    assert task_id == 907
    assert payload["selection_mode"] == "MISSING_EPISODES"
    async with sessions() as db:
        task = await db.get(TransferQueueTask, 907)
        assert task.status == "RUNNING"
    await engine.dispose()


@pytest.mark.asyncio
async def test_follow_candidate_switch_metric_is_set_when_new_share_replaces_pending_review(tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.ingest.channel_ingest_service import ChannelIngestService
    from app.models import import_all_models
    from app.models.channel import ChannelSetting
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from app.schemas.telegram_source import TelegramSourceMessage
    from app.scout.scout_service import ScoutService

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/candidate-switch-metric.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        old_resource = Resource(
            id=908, identity_key="908:old-candidate", tmdb_id=908, title="切源指标剧",
            media_type="tv", season=1, episode=2, episode_key="S01E02",
            cloud_name="guangya", share_url="https://pan.guangyapan.com/s/old-candidate",
            source_type="watchlist_scout",
        )
        db.add(old_resource)
        await db.flush()
        db.add(TransferQueueTask(
            id=908, task_type="TRANSFER", resource_id=old_resource.id,
            idempotency_key="old-review-908", status="PENDING",
            payload={"tmdb_id": 908, "title": "切源指标剧", "season": 1,
                    "episode_keys": ["S01E02"], "share_url": old_resource.share_url},
        ))
        source = TelegramSourceMessage(
            source_type="manual_forward",
            channel_id="candidate-switch-test",
            message_id=909,
            text="切源指标剧 S01E02 https://pan.guangyapan.com/s/new-candidate",
            is_forward=True,
            urls=["https://pan.guangyapan.com/s/new-candidate"],
            metadata={
                "tmdb_id": 908,
                "title": "切源指标剧",
                "season": 1,
                "episode_keys": ["S01E02"],
                "share_url": "https://pan.guangyapan.com/s/new-candidate",
            },
        )
        setting = ChannelSetting(
            channel_id="candidate-switch-test",
            role="MANUAL_INGEST",
            accept_forward=True,
            transfer_mode="AUTO",
        )
        result = await ChannelIngestService.process_source_message(db, source, channel_setting=setting)

    stats = ScoutService.summarize([result])
    assert result["queued"] is True
    assert result["candidate_switched"] is True
    assert stats["candidate_switched"] == 1
    await engine.dispose()
