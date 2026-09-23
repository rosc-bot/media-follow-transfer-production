import pytest

from app.workers.transfer_worker import run_transfer_worker


class RecordingWorker:
    def __init__(self):
        self.calls = 0

    async def process_once(self):
        self.calls += 1
        return self.calls == 1


@pytest.mark.asyncio
async def test_transfer_runtime_keeps_polling_and_uses_idle_interval():
    worker = RecordingWorker()
    delays = []

    async def stop_after_two_sleeps(delay):
        delays.append(delay)
        if len(delays) == 2:
            raise RuntimeError('stop test loop')

    with pytest.raises(RuntimeError, match='stop test loop'):
        await run_transfer_worker(worker=worker, poll_interval_seconds=7, sleep=stop_after_two_sleeps)

    assert worker.calls == 2
    assert delays == [0, 7]
