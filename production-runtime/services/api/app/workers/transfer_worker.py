import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Protocol

from app.core.config import get_settings
from app.core.database import AsyncSessionLocal
from app.core.logging import configure_logging
from app.follow.worker_heartbeat import (
    TRANSFER_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    write_transfer_worker_heartbeat,
)
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker

logger = logging.getLogger(__name__)


class QueueProcessor(Protocol):
    async def process_once(self) -> bool: ...


def _current_release_commit() -> str:
    configured = str(os.environ.get("RELEASE_COMMIT") or "").strip()
    if configured and configured.casefold() != "unknown":
        return configured
    try:
        marker = Path("/app/RELEASE_COMMIT").read_text(encoding="utf-8").strip()
    except OSError:
        marker = ""
    return marker or configured or "unknown"


async def _transfer_worker_heartbeat_loop(stop_event: asyncio.Event) -> None:
    settings = get_settings()
    release_commit = _current_release_commit()
    while not stop_event.is_set():
        try:
            await write_transfer_worker_heartbeat(
                AsyncSessionLocal,
                cloud_write_enabled=bool(settings.cloud_write_enabled),
                release_commit=release_commit,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Transfer worker heartbeat write failed")
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=TRANSFER_WORKER_HEARTBEAT_INTERVAL_SECONDS,
            )
        except TimeoutError:
            continue


async def recover_stale_tasks() -> int:
    async with AsyncSessionLocal() as db, db.begin():
        # Other slots/processes may still own fresh claims; never steal all locks.
        return await TransferQueueService.recover_stale(db)


async def run_once() -> bool:
    return await TransferQueueWorker(AsyncSessionLocal).process_once()


async def _poll_worker(
    worker: QueueProcessor,
    *,
    poll_interval_seconds: float,
    sleep: Callable[[float], Awaitable[object]],
    busy_delay: float,
) -> None:
    while True:
        try:
            processed = await worker.process_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unexpected error in transfer worker process_once, retrying in 5s...")
            await sleep(5.0)
            continue
        await sleep(busy_delay if processed else poll_interval_seconds)


async def _run_worker_slot(
    slot_id: int,
    poll_interval_seconds: float = 3.0,
    *,
    sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
) -> None:
    import socket
    if slot_id > 1:
        await sleep((slot_id - 1) * 1.2)
    worker_id = f"{socket.gethostname()}:{os.getpid()}:worker-{slot_id}"
    worker = TransferQueueWorker(AsyncSessionLocal, worker_id=worker_id)
    logger.info("Transfer worker slot %d (%s) running...", slot_id, worker_id)
    await _poll_worker(worker, poll_interval_seconds=poll_interval_seconds, sleep=sleep, busy_delay=0.5)


async def run_transfer_worker(
    *,
    worker: QueueProcessor | None = None,
    poll_interval_seconds: float | None = None,
    sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
) -> None:
    """Continuously consume the durable queue; default startup also recovers stale locks."""
    configure_logging()
    # Preserve the live three-slot default (3s) and the injected legacy
    # runner default (5s); explicit caller intervals are still honored.
    effective_interval = (5.0 if worker is not None else 3.0) if poll_interval_seconds is None else poll_interval_seconds
    if worker is not None:
        # Injected workers are self-contained: no real DB, heartbeat, or extra slots.
        await _poll_worker(worker, poll_interval_seconds=effective_interval, sleep=sleep, busy_delay=0)
        return
    logger.info("Transfer worker starting up (3-Worker Concurrency Enabled)...")
    heartbeat_stop = asyncio.Event()
    heartbeat_task = asyncio.create_task(_transfer_worker_heartbeat_loop(heartbeat_stop))
    slot_tasks = []
    try:
        recovered = await recover_stale_tasks()
        logger.info("Recovered %d stale transfer tasks", recovered)
        slot_tasks = [
            asyncio.create_task(_run_worker_slot(slot_id, poll_interval_seconds=effective_interval, sleep=sleep))
            for slot_id in (1, 2, 3)
        ]
        await asyncio.gather(*slot_tasks)
    finally:
        heartbeat_stop.set()
        heartbeat_task.cancel()
        for task in slot_tasks:
            task.cancel()
        await asyncio.gather(heartbeat_task, *slot_tasks, return_exceptions=True)


if __name__ == '__main__':
    asyncio.run(run_transfer_worker())
