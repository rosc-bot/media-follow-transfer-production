
from sqlalchemy.ext.asyncio import AsyncSession

from app.transfer.queue_service import TransferQueueService


class TransferRecovery:
    @staticmethod
    async def recover(db: AsyncSession, stale_after_seconds: int = 900) -> int:
        return await TransferQueueService.recover_stale(db, stale_after_seconds=stale_after_seconds)
