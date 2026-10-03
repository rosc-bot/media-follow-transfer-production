"""Poison-task isolation against a real in-memory SQLAlchemy database."""
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.exc import OperationalError

from app.core.database import Base
from app.models.bot_settings import BotSettings
from app.models.cloud import CloudConfig
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker

@pytest.fixture
async def sessions():
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()

async def enqueue_pair(sessions, disabled=False):
    async with sessions() as db, db.begin():
        db.add_all([BotSettings(key='global_pause',val='0'), BotSettings(key='transfer_paused',val='0')])
        if disabled:
            db.add(CloudConfig(name='guangya',enabled=False))
        good = await TransferQueueService.enqueue(db,resource_id=101,provider='dry-run',episode_keys=['S01E01'],
            payload={'expected_files':['S01E01.mkv']})
        bad = await TransferQueueService.enqueue(db,resource_id=102,provider='guangya',episode_keys=['S01E02'],
            payload={'expected_files':['S01E02.mkv']})
        bad.max_retries = 1
    return good.id, bad.id

@pytest.mark.asyncio
@pytest.mark.parametrize('disabled',[False,True])
async def test_missing_or_disabled_config_is_isolated_and_next_task_can_run(sessions, disabled):
    good_id,bad_id = await enqueue_pair(sessions,disabled)
    worker = TransferQueueWorker(sessions,worker_id='poison-test')
    assert await worker._claim_payload() is None
    async with sessions() as db:
        bad = await db.get(TransferQueueTask,bad_id)
        assert bad.status == 'PENDING'
        assert bad.attempt_count == 1
        assert bad.locked_by is None and bad.locked_at is None
        assert 'TASK_HYDRATION_ERROR' in bad.error_message
        assert ('禁用' if disabled else '未配置') in bad.error_message
    claimed = await worker._claim_payload()
    assert claimed is not None and claimed[0] == good_id
    async with sessions() as db:
        bad = await db.get(TransferQueueTask,bad_id)
        assert bad.attempt_count == 1

@pytest.mark.asyncio
async def test_partial_hydration_mutations_rollback_without_losing_claim_count(sessions,monkeypatch):
    _,bad_id = await enqueue_pair(sessions)
    async with sessions() as db,db.begin():
        db.add(Resource(id=102,identity_key='poison-partial',share_url='https://invalid.example/share',source_type='test',title='原始标题',cloud_name='guangya',media_type='tv',file_names=[]))
    worker = TransferQueueWorker(sessions,worker_id='poison-test')
    async def broken(db,task):
        resource = await db.get(Resource,task.resource_id)
        resource.title = '错误的半成品修改'
        task.payload = {'partial':'should rollback'}
        await db.flush()
        raise ValueError('invalid metadata auth_ref=secret-do-not-log')
    monkeypatch.setattr(worker,'_hydrate_payload',broken)
    assert await worker._claim_payload() is None
    async with sessions() as db:
        bad = await db.get(TransferQueueTask,bad_id)
        resource = await db.get(Resource,102)
        assert resource.title == '原始标题'
        assert 'partial' not in bad.payload
        assert bad.attempt_count == 1 and bad.status == 'PENDING'
        assert 'secret-do-not-log' not in bad.error_message

@pytest.mark.asyncio
async def test_database_failure_is_not_mislabeled_as_task_error(sessions,monkeypatch):
    _,bad_id = await enqueue_pair(sessions)
    worker = TransferQueueWorker(sessions,worker_id='poison-test')
    async def broken(db,task):
        raise OperationalError('SELECT',{},Exception('database unavailable'))
    monkeypatch.setattr(worker,'_hydrate_payload',broken)
    with pytest.raises(OperationalError):
        await worker._claim_payload()
    async with sessions() as db:
        bad = await db.get(TransferQueueTask,bad_id)
        assert bad.status == 'QUEUED' and bad.attempt_count == 0
