import logging
import os
import socket

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.follow.watchlist_service import WatchlistService
from app.models.resource import Resource
from app.transfer.notifier import TransferNotifier
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.queue_service import TransferQueueService

logger = logging.getLogger(__name__)


class TransferQueueWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        orchestrator: TransferOrchestrator | None = None,
        worker_id: str | None = None,
        notifier: TransferNotifier | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.orchestrator = orchestrator or TransferOrchestrator()
        self.worker_id = worker_id or f'{socket.gethostname()}:{os.getpid()}'
        self.notifier = notifier if notifier is not None else TransferNotifier()

    async def process_once(self) -> bool:
        async with self.session_factory() as db, db.begin():
            task = await TransferQueueService.claim_next(db, worker_id=self.worker_id)
            if not task:
                return False
            try:
                outcome = await self.orchestrator.execute(task.payload)
                transfer_result = {
                    'verified': outcome.verified,
                    'remote_folder_id': outcome.remote_folder_id,
                    'remote_files': list(outcome.remote_files),
                }
                await TransferQueueService.mark_completed(db, task, transfer_result)
                resource = await db.get(Resource, task.resource_id)
                if resource and resource.tmdb_id and resource.season:
                    keys = list(task.payload.get('episode_keys') or ([resource.episode_key] if resource.episode_key else []))
                    if keys:
                        await WatchlistService.mark_collected(db, tmdb_id=resource.tmdb_id, season=resource.season, episode_keys=keys)
                if self.notifier:
                    await self.notifier.notify_success(
                        task_payload=task.payload,
                        transfer_result=transfer_result,
                        resource=resource,
                    )
            except Exception as exc:
                logger.exception('transfer task %s failed', task.id)
                await TransferQueueService.mark_failed(db, task, str(exc))
                if self.notifier:
                    try:
                        resource = await db.get(Resource, task.resource_id)
                        await self.notifier.notify_failure(
                            task_payload=task.payload,
                            error_message=str(exc),
                            resource=resource,
                            attempts=getattr(task, 'attempt_count', 1),
                        )
                    except Exception as notif_err:  # noqa: BLE001
                        logger.warning('Failed to send failure notification for task %s: %s', task.id, notif_err)
        return True
