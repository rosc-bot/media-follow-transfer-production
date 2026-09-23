import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path

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
        return await TransferQueueService.recover_stale(db)


async def run_once() -> bool:
    return await TransferQueueWorker(AsyncSessionLocal).process_once()


async def run_transfer_worker(
    *,
    worker: TransferQueueWorker | object | None = None,
    poll_interval_seconds: float = 5,
    sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
) -> None:
    """Continuously consume the durable queue; default startup also recovers stale locks."""
    configure_logging()
    logger.info("Transfer worker starting up...")
    heartbeat_task = None
    heartbeat_stop = None
    if worker is None:
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(_transfer_worker_heartbeat_loop(heartbeat_stop))
        recovered = await recover_stale_tasks()
        logger.info("Recovered %d stale transfer tasks", recovered)
        worker = TransferQueueWorker(AsyncSessionLocal)
    try:
        while True:
            try:
                processed = await worker.process_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unexpected error in transfer worker process_once, retrying in 5s...")
                await sleep(5.0)
                continue
            await sleep(0 if processed else poll_interval_seconds)
    finally:
        if heartbeat_task is not None and heartbeat_stop is not None:
            heartbeat_stop.set()
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)


if __name__ == '__main__':
    asyncio.run(run_transfer_worker())
