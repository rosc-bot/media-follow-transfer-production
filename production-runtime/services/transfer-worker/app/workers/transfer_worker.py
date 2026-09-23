import asyncio
import logging
from collections.abc import Awaitable, Callable

from app.core.database import AsyncSessionLocal
from app.core.logging import configure_logging
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker

logger = logging.getLogger(__name__)


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
    if worker is None:
        recovered = await recover_stale_tasks()
        logger.info("Recovered %d stale transfer tasks", recovered)
        worker = TransferQueueWorker(AsyncSessionLocal)
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


if __name__ == '__main__':
    asyncio.run(run_transfer_worker())
