import asyncio

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.follow.bot_settings_service import BotSettingsService
from app.follow.calendar_service import CalendarService
from app.follow.follow_worker import run_follow_cycle
from app.models.bot_settings import BotSettings
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.monitor.event_router import EventRouter
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker
from app.transfer.status import TransferOutcome, TransferStatus


class RecordingCalendar:
    def __init__(self):
        self.calls = 0

    async def fetch_schedule(self, tmdb_id, season):
        self.calls += 1
        return {"total_episodes": 1, "last_aired_episode": 1}


class RecordingScout:
    def __init__(self):
        self.calls = 0

    async def scout_missing(self, *args, **kwargs):
        self.calls += 1
        return []


class RecordingAdapter:
    def __init__(self):
        self.calls = 0

    async def transfer(self, payload):
        self.calls += 1
        return TransferOutcome(True, True, "folder", tuple(payload.get("expected_files", [])))


class DummyChat:
    id = -1001234567890
    title = "普通群"
    broadcast = False


class DummyEvent:
    chat_id = DummyChat.id
    chat = DummyChat()
    message = object()

    async def get_chat(self):
        return self.chat


async def _sessions(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/pause.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


@pytest.mark.asyncio
async def test_missing_split_settings_fail_closed_by_default(tmp_path):
    engine, sessions = await _sessions(tmp_path)
    async with sessions() as db:
        assert await BotSettingsService.is_follow_paused(db) is True
        assert await BotSettingsService.is_transfer_paused(db) is True
    await engine.dispose()


@pytest.mark.asyncio
async def test_follow_paused_skips_tmdb_and_scout_work(tmp_path):
    engine, sessions = await _sessions(tmp_path)
    calendar = RecordingCalendar()
    scout = RecordingScout()
    async with sessions() as db, db.begin():
        db.add(BotSettings(key="global_pause", val="0"))
        db.add(BotSettings(key="follow_paused", val="1"))
        db.add(SeriesWatchlist(tmdb_id=100, title="暂停剧", season=1, status="FOLLOWING", collected_episodes=[]))
    async with sessions() as db, db.begin():
        result = await run_follow_cycle(db, calendar=CalendarService(calendar), scout=scout)
    assert result == {"synced_watchlists": 0, "scout_jobs": 0}
    assert calendar.calls == 0
    assert scout.calls == 0
    await engine.dispose()


@pytest.mark.asyncio
async def test_transfer_paused_does_not_claim_or_execute_task(tmp_path):
    engine, sessions = await _sessions(tmp_path)
    adapter = RecordingAdapter()
    async with sessions() as db, db.begin():
        db.add(BotSettings(key="global_pause", val="0"))
        db.add(BotSettings(key="transfer_paused", val="1"))
        task = await TransferQueueService.enqueue(
            db, resource_id=91, provider="dry-run", episode_keys=["S01E01"], payload={"expected_files": ["a.mkv"]}
        )
        task_id = task.id
    worker = TransferQueueWorker(sessions, TransferOrchestrator({"dry-run": adapter}), worker_id="test")
    assert await worker.process_once() is False
    assert adapter.calls == 0
    async with sessions() as db:
        task = await db.get(TransferQueueTask, task_id)
        assert task.status == TransferStatus.QUEUED
        assert task.attempt_count == 0
    await engine.dispose()


@pytest.mark.asyncio
async def test_follow_can_run_while_transfer_remains_paused(tmp_path):
    engine, sessions = await _sessions(tmp_path)
    calendar = RecordingCalendar()
    scout = RecordingScout()
    adapter = RecordingAdapter()
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="0"),
            BotSettings(key="follow_paused", val="0"),
            BotSettings(key="transfer_paused", val="1"),
            SeriesWatchlist(tmdb_id=101, title="安全试跑剧", season=1, status="FOLLOWING", collected_episodes=[]),
        ])
        task = await TransferQueueService.enqueue(
            db, resource_id=92, provider="dry-run", episode_keys=["S01E01"], payload={"expected_files": ["b.mkv"]}
        )
        task_id = task.id
    async with sessions() as db, db.begin():
        result = await run_follow_cycle(db, calendar=CalendarService(calendar), scout=scout)
    assert result["synced_watchlists"] == 1
    assert calendar.calls == 1
    worker = TransferQueueWorker(sessions, TransferOrchestrator({"dry-run": adapter}), worker_id="test")
    assert await worker.process_once() is False
    assert adapter.calls == 0
    async with sessions() as db:
        task = await db.get(TransferQueueTask, task_id)
        assert task.status == TransferStatus.QUEUED
        assert task.attempt_count == 0
    await engine.dispose()


@pytest.mark.asyncio
async def test_global_pause_remains_an_effective_pause_for_both_workers(tmp_path):
    engine, sessions = await _sessions(tmp_path)
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="1"),
            BotSettings(key="follow_paused", val="0"),
            BotSettings(key="transfer_paused", val="0"),
        ])
    async with sessions() as db:
        assert await BotSettingsService.is_follow_paused(db) is True
        assert await BotSettingsService.is_transfer_paused(db) is True
    await engine.dispose()


@pytest.mark.asyncio
async def test_telegram_router_keeps_summary_routing_when_both_workers_paused(tmp_path):
    engine, sessions = await _sessions(tmp_path)
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="0"),
            BotSettings(key="follow_paused", val="1"),
            BotSettings(key="transfer_paused", val="1"),
        ])
    summary_queue = asyncio.Queue()
    resource_queue = asyncio.Queue()
    router = EventRouter(summary_queue, resource_queue, {})
    await router.route_event(DummyEvent())
    assert summary_queue.qsize() == 1
    assert resource_queue.qsize() == 0
    await engine.dispose()
