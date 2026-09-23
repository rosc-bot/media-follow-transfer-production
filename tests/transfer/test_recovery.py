from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.bot_settings import BotSettings
from app.models.transfer import TransferQueueTask
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker
from app.transfer.status import TransferOutcome, TransferStatus


class FakeAdapter:
    async def transfer(self, payload):
        return TransferOutcome(True, True, 'folder', tuple(payload.get('expected_files', [])))


@pytest.mark.asyncio
async def test_stale_running_task_recovers_and_completes_after_worker_restart(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/recovery.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add_all([BotSettings(key="global_pause", val="0"), BotSettings(key="transfer_paused", val="0")])
        task = await TransferQueueService.enqueue(db, resource_id=1, provider='dry-run', episode_keys=['S01E01'], payload={'expected_files': ['a.mkv']})
        task.status = TransferStatus.RUNNING
        task.locked_at = datetime.now(UTC) - timedelta(seconds=901)
        task.locked_by = 'crashed-worker'
    async with sessions() as db, db.begin():
        assert await TransferQueueService.recover_stale(db, stale_after_seconds=900) == 1
    worker = TransferQueueWorker(sessions, TransferOrchestrator({'dry-run': FakeAdapter()}), worker_id='restarted-worker')
    assert await worker.process_once() is True
    async with sessions() as db:
        task = await db.scalar(select(TransferQueueTask).where(TransferQueueTask.id == task.id))
        assert task.status == TransferStatus.COMPLETED
        assert task.locked_by is None
    await engine.dispose()
