from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.transfer import TransferQueueTask
from app.transfer.queue_service import TransferQueueService
from app.transfer.status import SUCCESS_TERMINAL_STATUSES, is_success_terminal

router = APIRouter(prefix='/transfer', tags=['transfer'])


@router.get('/queue')
async def list_queue(db: AsyncSession = Depends(get_db)):  # noqa: B008
    tasks = (await db.scalars(
        select(TransferQueueTask).order_by(TransferQueueTask.id.desc()).limit(100)
    )).all()
    return [
        {
            'id': task.id,
            'resource_id': task.resource_id,
            'status': task.status,
            'attempt_count': task.attempt_count,
            'is_success_terminal': is_success_terminal(task.status),
        }
        for task in tasks
    ]


@router.get('/queue/stats')
async def queue_stats(db: AsyncSession = Depends(get_db)):  # noqa: B008
    counts = await TransferQueueService.status_counts(db)
    return {
        'counts': counts,
        'success_terminal_statuses': sorted(str(status) for status in SUCCESS_TERMINAL_STATUSES),
        'success_terminal_count': sum(
            counts.get(str(status), 0) for status in SUCCESS_TERMINAL_STATUSES
        ),
    }
