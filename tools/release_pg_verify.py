"""Run read/write verification ONLY against a disposable release PostgreSQL DB."""
import asyncio
import os

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models import import_all_models
from app.models.bot_settings import BotSettings
from app.models.transfer import TransferQueueTask
from app.models.resource import Resource
from app.follow.bot_settings_service import BotSettingsService
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker


async def verify():
    url = os.environ['RELEASE_TEST_DATABASE_URL']
    if not url.endswith('/verification'):
        raise RuntimeError('Only the disposable verification database is allowed')
    import_all_models()
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with sessions() as db, db.begin():
            db.add_all([BotSettings(key='global_pause',val='0'),BotSettings(key='transfer_paused',val='0')])
            db.add_all([
                Resource(id=11,identity_key='verification-good',share_url='https://invalid.example/verification/good',source_type='unit-test',title='verification-good',tmdb_id=7,media_type='tv',season=1,episode_key='S01E01',cloud_name='dry-run'),
                Resource(id=12,identity_key='verification-bad',share_url='https://invalid.example/verification/bad',source_type='unit-test',title='verification-bad',tmdb_id=7,media_type='tv',season=1,episode_key='S01E02',cloud_name='guangya'),
            ])
            await db.flush()
            good = await TransferQueueService.enqueue(db, resource_id=11, provider='dry-run', episode_keys=['S01E01'], payload={'preflight_classification':'AUTO_SAFE'})
            good_id = good.id
            bad = await TransferQueueService.enqueue(db, resource_id=12, provider='guangya', episode_keys=['S01E02'], payload={'preflight_classification':'AUTO_SAFE'})
            bad_id = bad.id
        worker = TransferQueueWorker(sessions, worker_id='isolated-postgres-verification')
        assert await worker.process_once() is False
        async with sessions() as db, db.begin():
            bad = await db.get(TransferQueueTask,bad_id)
            assert bad.status == 'PENDING' and bad.attempt_count == 1
            assert bad.locked_at is None and bad.locked_by is None
            assert bad.payload['preflight_classification'] == 'NEEDS_REVIEW'
            claim = await TransferQueueService.claim_next(db,worker_id='isolated-postgres-verification')
            assert claim is not None and claim.id == good_id
        async with sessions() as retained:
            row = await retained.get(BotSettings,'transfer_paused')
            assert row.val == '0'
            async with sessions() as writer, writer.begin():
                await BotSettingsService.set(writer,'transfer_paused','1')
            assert await BotSettingsService.get(retained,'transfer_paused') == '1'
        print('POSTGRES_VERIFICATION_PASSED: poison isolation, next task claim, pause visibility')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(verify())
