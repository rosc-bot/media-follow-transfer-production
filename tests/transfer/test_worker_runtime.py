import asyncio
from unittest.mock import AsyncMock

import pytest

from app.follow import transfer_worker as legacy_runtime
from app.workers import transfer_worker as runtime


class RecordingWorker:
    def __init__(self):
        self.calls = 0

    async def process_once(self):
        self.calls += 1
        return self.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('entrypoint', [runtime, legacy_runtime])
async def test_transfer_runtime_keeps_polling_and_uses_idle_interval(monkeypatch, entrypoint):
    worker = RecordingWorker()
    delays = []
    forbidden = AsyncMock(side_effect=AssertionError('injected worker must not access default startup'))
    monkeypatch.setattr(runtime, 'recover_stale_tasks', forbidden)
    monkeypatch.setattr(runtime, '_transfer_worker_heartbeat_loop', forbidden)

    async def stop_after_two_sleeps(delay):
        delays.append(delay)
        if len(delays) == 2:
            raise RuntimeError('stop test loop')

    with pytest.raises(RuntimeError, match='stop test loop'):
        await entrypoint.run_transfer_worker(worker=worker, poll_interval_seconds=7, sleep=stop_after_two_sleeps)

    assert worker.calls == 2
    assert delays == [0, 7]
    forbidden.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('entrypoint', [runtime, legacy_runtime])
async def test_transfer_runtime_recovers_process_errors_and_propagates_cancel(monkeypatch, entrypoint):
    worker = RecordingWorker()
    worker.process_once = AsyncMock(side_effect=[ValueError('transient'), False, asyncio.CancelledError()])
    delays = []

    async def record_sleep(delay):
        delays.append(delay)

    forbidden = AsyncMock(side_effect=AssertionError('unexpected default startup'))
    monkeypatch.setattr(runtime, 'recover_stale_tasks', forbidden)
    monkeypatch.setattr(runtime, '_transfer_worker_heartbeat_loop', forbidden)
    with pytest.raises(asyncio.CancelledError):
        await entrypoint.run_transfer_worker(worker=worker, poll_interval_seconds=9, sleep=record_sleep)
    assert worker.process_once.await_count == 3
    assert delays == [5.0, 9]
    forbidden.assert_not_awaited()


@pytest.mark.asyncio
async def test_default_runtime_honors_interval_and_cleans_all_background_tasks(monkeypatch):
    started = []
    stopped = []
    heartbeats = []
    heartbeat_ready = asyncio.Event()
    slots_ready = asyncio.Event()

    async def heartbeat(stop):
        heartbeats.append(stop)
        heartbeat_ready.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.append('heartbeat')

    async def slot(slot_id, poll_interval_seconds=3.0, **kwargs):
        started.append((slot_id, poll_interval_seconds))
        if len(started) == 3:
            slots_ready.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.append(slot_id)

    recover = AsyncMock(return_value=2)
    monkeypatch.setattr(runtime, 'recover_stale_tasks', recover)
    monkeypatch.setattr(runtime, '_transfer_worker_heartbeat_loop', heartbeat)
    monkeypatch.setattr(runtime, '_run_worker_slot', slot)
    task = asyncio.create_task(runtime.run_transfer_worker(poll_interval_seconds=11))
    try:
        await asyncio.wait_for(heartbeat_ready.wait(), timeout=1)
        await asyncio.wait_for(slots_ready.wait(), timeout=1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    recover.assert_awaited_once()
    assert started == [(1, 11), (2, 11), (3, 11)]
    assert set(stopped) == {'heartbeat', 1, 2, 3}
    assert heartbeats[0].is_set()


@pytest.mark.asyncio
async def test_startup_recovery_failure_does_not_leak_heartbeat(monkeypatch):
    heartbeat_tasks = []

    async def heartbeat(stop):
        heartbeat_tasks.append(asyncio.current_task())
        await asyncio.Event().wait()

    async def fail_recovery():
        await asyncio.sleep(0)  # Let the heartbeat start before recovery fails.
        raise RuntimeError('recovery database unavailable')

    monkeypatch.setattr(runtime, 'recover_stale_tasks', fail_recovery)
    monkeypatch.setattr(runtime, '_transfer_worker_heartbeat_loop', heartbeat)
    try:
        with pytest.raises(RuntimeError, match='recovery database unavailable'):
            await runtime.run_transfer_worker()
        assert heartbeat_tasks and all(task.done() for task in heartbeat_tasks)
    finally:
        for task in heartbeat_tasks:
            task.cancel()
        await asyncio.gather(*heartbeat_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_startup_recovery_preserves_live_claims(monkeypatch):
    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def begin(self):
            return self

    async def recover(db, *, stale_after_seconds=900):
        assert stale_after_seconds == 900
        return 0

    monkeypatch.setattr(runtime, 'AsyncSessionLocal', Session)
    monkeypatch.setattr(runtime.TransferQueueService, 'recover_stale', recover)
    assert await runtime.recover_stale_tasks() == 0
