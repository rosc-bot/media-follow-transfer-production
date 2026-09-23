from datetime import UTC, datetime, timedelta

import httpx
import pytest


def test_provider_transient_classifier_distinguishes_transport_from_review():
    from app.transfer.provider_network_health import is_retryable_provider_failure

    assert is_retryable_provider_failure("SHARE_READ_NETWORK_TIMEOUT", stage="SHARE_PROBE")
    assert is_retryable_provider_failure("BATCH_PREFLIGHT_TIMEOUT", stage="SHARE_PROBE")
    assert is_retryable_provider_failure(
        "PHYSICAL_CLOUD_SCAN_API_ERROR", detail="ConnectTimeout", stage="CLOUD_PRESENCE_SCAN"
    )
    assert is_retryable_provider_failure("REMOTE_5XX", stage="CLOUD_PRESENCE_SCAN")
    assert not is_retryable_provider_failure(
        "PHYSICAL_CLOUD_SCAN_API_ERROR", detail="PAGINATION_INCOMPLETE", stage="CLOUD_PRESENCE_SCAN"
    )
    assert not is_retryable_provider_failure("CLOUD_MULTIVERSION_CONFLICT", stage="CLOUD_PRESENCE_SCAN")


def test_provider_network_retry_backoff_is_bounded():
    from app.transfer.provider_network_health import provider_backoff_seconds

    assert [provider_backoff_seconds(n) for n in range(1, 6)] == [60, 180, 300, 600, 900]
    assert provider_backoff_seconds(99) == 900


@pytest.mark.asyncio
async def test_provider_breaker_counts_distinct_tasks_and_recovers_after_read_only_probe(tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.transfer.provider_network_health import (
        complete_provider_health_probe,
        provider_claim_gate,
        record_provider_network_failure,
    )

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/provider-breaker.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 9, 23, 12, tzinfo=UTC)

    async with sessions() as db, db.begin():
        assert (await record_provider_network_failure(db, task_id=1350, now=now))["state"] == "CLOSED"
        assert (await record_provider_network_failure(db, task_id=1350, now=now + timedelta(seconds=1)))["distinct_tasks"] == 1
        assert (await record_provider_network_failure(db, task_id=1351, now=now + timedelta(seconds=2)))["state"] == "CLOSED"
        state = await record_provider_network_failure(db, task_id=1352, now=now + timedelta(seconds=3))
        assert state["state"] == "PROVIDER_DEGRADED"
        assert state["distinct_tasks"] == 3
        assert await provider_claim_gate(db, now=now + timedelta(seconds=4)) == "WAIT"
        assert await provider_claim_gate(db, now=now + timedelta(seconds=184)) == "PROBE"

    async with sessions() as db, db.begin():
        await complete_provider_health_probe(db, success=True, now=now + timedelta(seconds=185))
        assert await provider_claim_gate(db, now=now + timedelta(seconds=186)) == "ALLOW"
    await engine.dispose()


@pytest.mark.asyncio
async def test_worker_probes_after_cooldown_before_resuming_claims(tmp_path):
    import json
    from datetime import UTC, datetime, timedelta

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.bot_settings import BotSettings
    from app.transfer.queue_worker import TransferQueueWorker

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/provider-health-probe.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    cooldown_expired_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    async with sessions() as db, db.begin():
        db.add(BotSettings(
            key="transfer_provider_health_guangya",
            val=json.dumps({"state": "PROVIDER_DEGRADED", "cooldown_until": cooldown_expired_at, "failures": []}),
        ))
    worker = TransferQueueWorker(sessions, worker_id="probe-worker")
    probes = 0

    async def successful_probe():
        nonlocal probes
        probes += 1
        return True

    worker._provider_health_probe = successful_probe
    assert await worker._provider_claim_allowed() is True
    assert probes == 1
    async with sessions() as db:
        row = await db.get(BotSettings, "transfer_provider_health_guangya")
        assert json.loads(row.val)["state"] == "CLOSED"
    await engine.dispose()


@pytest.mark.asyncio
async def test_cloud_read_timeout_is_classified_as_transient_network_error():
    from app.follow.physical_cloud_inventory import PhysicalCloudInventoryScanner

    async def timeout_page(_parent_id, _page, _page_size):
        raise httpx.ConnectTimeout("timed out")

    scanner = PhysicalCloudInventoryScanner(timeout_page, rate_limit_seconds=0)
    result = await scanner.scan(tmdb_id=77, series_root_id="root", relevant_seasons=[1])
    assert result.scan_status == "TIMEOUT_UNVERIFIED"
    assert result.error == "NETWORK_TIMEOUT:ConnectTimeout"


@pytest.mark.asyncio
async def test_batch_network_timeout_becomes_retry_wait_without_global_pause(tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.bot_settings import BotSettings
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from app.transfer.queue_worker import TransferQueueWorker

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/network-retry.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    payload = {"selection_mode": "MISSING_EPISODES", "episode_keys": ["S01E08", "S01E09"], "provider": "guangya"}
    async with sessions() as db, db.begin():
        db.add(BotSettings(key="transfer_paused", val="0"))
        db.add(BotSettings(key="global_pause", val="0"))
        db.add(Resource(
            id=990, identity_key="990:guangya:S01E08", tmdb_id=990, title="网络重试剧",
            media_type="tv", season=1, episode=8, episode_key="S01E08",
            share_url="https://example.invalid/share", source_type="watchlist_scout",
        ))
        db.add(TransferQueueTask(
            id=990, task_type="TRANSFER", resource_id=990, idempotency_key="batch-network-990",
            status="RUNNING", attempt_count=1, locked_by="network-worker", payload=payload,
        ))

    worker = TransferQueueWorker(sessions, worker_id="network-worker")

    async def fail_batch(_db, *, task, resource, payload):
        payload["preflight_stage"] = "SHARE_PROBE"
        raise httpx.ReadTimeout("provider response timeout")

    worker._batch_episode_presence_preflight = fail_batch
    result = await worker._runtime_final_preflight(task_id=990, resource_id=990, payload=dict(payload))
    assert result is None

    async with sessions() as db:
        task = await db.get(TransferQueueTask, 990)
        assert task.status == "RETRY_WAIT"
        assert task.locked_at is None and task.locked_by is None
        assert task.attempt_count == 0
        assert task.payload["preflight_reason"] == "PROVIDER_NETWORK_TIMEOUT"
        assert task.payload["preflight_stage"] == "SHARE_PROBE"
        assert task.payload["preflight_network_attempts"] == 1
        assert task.next_run_at > datetime.now(UTC).replace(tzinfo=None)
        pause = await db.get(BotSettings, "transfer_paused")
        assert pause.val == "0"
    await engine.dispose()


@pytest.mark.asyncio
async def test_non_network_cloud_scan_failure_remains_pending_review(tmp_path):
    from datetime import UTC, datetime

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models import import_all_models
    from app.models.resource import Resource
    from app.models.transfer import TransferQueueTask
    from app.transfer.queue_worker import TransferQueueWorker

    import_all_models()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/network-review.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    payload = {"selection_mode": "MISSING_EPISODES", "episode_keys": ["S01E08"], "provider": "guangya"}
    async with sessions() as db, db.begin():
        db.add(Resource(
            id=991, identity_key="991:guangya:S01E08", tmdb_id=991, title="账本冲突剧",
            media_type="tv", season=1, episode=8, episode_key="S01E08",
            share_url="https://example.invalid/share", source_type="watchlist_scout",
        ))
        db.add(TransferQueueTask(
            id=991, task_type="TRANSFER", resource_id=991, idempotency_key="batch-review-991",
            status="RUNNING", locked_by="network-worker", locked_at=datetime.now(UTC), payload=payload,
        ))
    worker = TransferQueueWorker(sessions, worker_id="network-worker")

    async def failed_scan(_db, *, task, resource, payload):
        return {
            "classification": "NEEDS_REVIEW",
            "reason": "PHYSICAL_CLOUD_SCAN_API_ERROR",
            "detail": "PAGINATION_INCOMPLETE",
            "preflight_stage": "CLOUD_PRESENCE_SCAN",
        }

    worker._batch_episode_presence_preflight = failed_scan
    await worker._runtime_final_preflight(task_id=991, resource_id=991, payload=dict(payload))
    async with sessions() as db:
        task = await db.get(TransferQueueTask, 991)
        assert task.status == "PENDING"
        assert task.payload["preflight_reason"] == "PHYSICAL_CLOUD_SCAN_API_ERROR"
        assert task.locked_at is None and task.locked_by is None
    await engine.dispose()
