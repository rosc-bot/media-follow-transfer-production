"""Phase 2D Scout statistics and Canary pause-isolation contracts."""

import pytest

from app.scout.scout_service import ScoutService
from app.transfer.queue_worker import TransferQueueWorker


def test_scout_summary_separates_eligibility_new_and_deduplicated_tasks():
    stats = ScoutService.summarize([
        {
            "local_status": "LOCAL_MATCH", "framehdr_status": "NOT_ATTEMPTED", "final_status": "QUEUED",
            "transfer_eligible": True, "new_queue_task_created": False,
            "deduplicated_existing_task": True, "queue_reused": False,
        },
        {
            "local_status": "LOCAL_NO_MATCH", "framehdr_status": "FRAMEHDR_MATCH", "final_status": "QUEUED",
            "transfer_eligible": True, "new_queue_task_created": True,
            "deduplicated_existing_task": False, "queue_reused": False,
        },
    ])
    assert stats["transfer_eligible"] == 2
    assert stats["new_queue_tasks_created"] == 1
    assert stats["deduplicated_existing_tasks"] == 1
    assert stats["queue_reused"] == 0
    assert "queued" not in stats


@pytest.mark.asyncio
async def test_single_task_obeys_pause_without_canary_override(monkeypatch):
    worker = TransferQueueWorker(session_factory=None)  # no DB should be opened before pause refusal

    async def paused(_db):
        return True

    monkeypatch.setattr("app.transfer.queue_worker.BotSettingsService.is_transfer_paused", paused)
    class Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def begin(self): return self
    worker.session_factory = lambda: Session()
    result = await worker.execute_single_task(9)
    assert result == {"executed": False, "reason": "transfer_paused=1"}


@pytest.mark.asyncio
async def test_canary_override_requires_process_gate(monkeypatch):
    worker = TransferQueueWorker(session_factory=None)

    async def paused(_db):
        return True

    monkeypatch.setattr("app.transfer.queue_worker.BotSettingsService.is_transfer_paused", paused)
    monkeypatch.delenv("CANARY_CLOUD_WRITE_ENABLED", raising=False)
    class Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def begin(self): return self
    worker.session_factory = lambda: Session()
    result = await worker.execute_single_task(9, canary_override_pause=True)
    assert result == {"executed": False, "reason": "canary_override_not_authorized"}
