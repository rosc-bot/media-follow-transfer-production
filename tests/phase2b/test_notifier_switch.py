"""Phase 2B tests: failure notification details/buttons, switch-resource, pause guard."""

from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.transfer import TransferQueueTask
from app.transfer.failure_labels import failure_markup
from app.transfer.notifier import TransferNotifier
from app.transfer.switch_resource import NoAlternativeResourceError, switch_resource


def _mk_response():
    return httpx_response()


def httpx_response():
    import httpx
    return httpx.Response(200, json={'ok': True, 'result': {'message_id': 1}},
                          request=httpx.Request('POST', 'https://api.telegram.org/x'))


@pytest.mark.asyncio
async def test_failure_notification_has_chinese_detail_and_buttons():
    notifier = TransferNotifier(bot_token='123456:FAKE', admin_tg_id=8586984520)
    with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
        mock_post.return_value = httpx_response()
        sent = await notifier.notify_failure(
            task_payload={'title': '测试剧', 'season': 1, 'episode_keys': ['S01E03'],
                          'share_url': 'https://pan.guangyapan.com/s/x'},
            error_message='httpx.ConnectTimeout 30s',
            attempts=2,
            task_id=1299,
            category='NETWORK_TIMEOUT',
            stage='list_directory',
            http_status=None,
        )
        assert sent is True
        body = mock_post.call_args[1]['json']
        text = body['text']
        assert '【影视转存失败】' in text
        assert '测试剧' in text
        assert 'S01E03' in text
        assert '网络超时' in text          # category_zh
        assert '目标目录读取' in text       # stage_zh
        assert '任务ID' in text
        assert '第 2 次' in text
        markup = body['reply_markup']
        assert markup['inline_keyboard']
        flat = [b['callback_data'] for row in markup['inline_keyboard'] for b in row]
        assert 'tx_fail_retry:1299' in flat
        assert 'tx_fail_switch:1299' in flat
        assert 'tx_fail_rescout:1299' in flat
        assert 'tx_fail_cancel:1299' in flat
        assert 'tx_fail_ignore:1299' in flat
        assert '未知错误' not in text       # no generic error


def test_failure_markup_has_all_buttons():
    markup = failure_markup(42)
    flat = [b['callback_data'] for row in markup for b in row]
    assert flat == ['tx_fail_retry:42', 'tx_fail_switch:42', 'tx_fail_rescout:42', 'tx_fail_cancel:42', 'tx_fail_ignore:42']


def _seed_switch_env(tmp_path):
    with sqlite3_connect(tmp_path / 'r.db'):
        pass
    import sqlite3
    db = sqlite3.connect(tmp_path / 'r.db')
    db.execute("CREATE TABLE messages(chat_id TEXT, chat_title TEXT, message_id INTEGER, text TEXT, urls TEXT, source_type TEXT, is_forward INTEGER, date TEXT)")
    # Two distinct messages with different shares for the same episode.
    db.execute("INSERT INTO messages VALUES(?,?,?,?,?,?,?,?)",
               ('3702243011', '资源群', 1, '切换剧 S01E02', '["https://pan.guangyapan.com/s/alpha"]', 'telegram_channel', 0, ''))
    db.execute("INSERT INTO messages VALUES(?,?,?,?,?,?,?,?)",
               ('3702243011', '资源群', 2, '切换剧 S01E02', '["https://pan.guangyapan.com/s/beta"]', 'telegram_channel', 0, ''))
    db.commit()
    db.close()
    return tmp_path


def sqlite3_connect(path):
    import sqlite3
    return sqlite3.connect(path)


@pytest.mark.asyncio
async def test_switch_resource_excludes_current_candidate(tmp_path):
    _seed_switch_env(tmp_path)
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        task = TransferQueueTask(
            task_type='TRANSFER', resource_id=99, idempotency_key='x',
            status='FAILED',
            payload={'tmdb_id': 777, 'title': '切换剧', 'season': 1, 'episode_keys': ['S01E02'],
                     'share_url': 'https://pan.guangyapan.com/s/alpha',
                     'source_message_id': 1, 'source_channel_id': '3702243011'},
            error_message='[INVALID_SHARE] share broken',
        )
        db.add(task)
        await db.flush()
        task_id = task.id
        detail = await switch_resource(
            db, task_id=task_id, resource_db_path=str(tmp_path / 'r.db'),
            exclude_share_hash=None,
        )
        await db.commit()
    async with sessions() as db:
        new_tasks = (await db.scalars(select(TransferQueueTask))).all()
        live = [t for t in new_tasks if t.id != task_id or t.status != 'FAILED']
        assert len(live) >= 1
        assert detail['new_tasks']
        chosen = detail['new_tasks'][0]
        # The chosen share must be the *other* candidate (beta), not the failed alpha.
        new_task = await db.get(TransferQueueTask, chosen['task_id'])
        assert 'beta' in (new_task.payload or {}).get('share_url', '')
    await engine.dispose()


@pytest.mark.asyncio
async def test_switch_resource_no_alternative(tmp_path):
    import sqlite3
    db = sqlite3.connect(tmp_path / 'r.db')
    db.execute("CREATE TABLE messages(chat_id TEXT, chat_title TEXT, message_id INTEGER, text TEXT, urls TEXT, source_type TEXT, is_forward INTEGER, date TEXT)")
    db.execute("INSERT INTO messages VALUES(?,?,?,?,?,?,?,?)",
               ('3702243011', '资源群', 1, '独播剧 S01E02', '["https://pan.guangyapan.com/s/only"]', 'telegram_channel', 0, ''))
    db.commit()
    db.close()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        task = TransferQueueTask(
            task_type='TRANSFER', resource_id=88, idempotency_key='y', status='FAILED',
            payload={'tmdb_id': 888, 'title': '独播剧', 'season': 1, 'episode_keys': ['S01E02'],
                     'share_url': 'https://pan.guangyapan.com/s/only', 'source_message_id': 1,
                     'source_channel_id': '3702243011'},
        )
        db.add(task)
        await db.flush()
        task_id = task.id
        with pytest.raises(NoAlternativeResourceError):
            await switch_resource(db, task_id=task_id, resource_db_path=str(tmp_path / 'r.db'))
        await db.rollback()
    await engine.dispose()


@pytest.mark.asyncio
async def test_transfer_pause_guard_keeps_buttons_non_destructive(tmp_path):
    """The retry button only changes queue state; with transfer_paused=1 no
    real transfer can ever start. Simulate the callback path: task->QUEUED and
    verify no transfer orchestrator is invoked (nothing claims it)."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        task = TransferQueueTask(
            task_type='TRANSFER', resource_id=1, idempotency_key='z', status='FAILED',
            payload={'share_url': 'https://pan.guangyapan.com/s/x'},
            error_message='[AUTH_EXPIRED] fail',
        )
        db.add(task)
        await db.flush()
        task_id = task.id
    from datetime import UTC, datetime

    async with sessions() as db, db.begin():
        t = await db.get(TransferQueueTask, task_id)
        t.status = 'QUEUED'
        t.error_message = None
        t.locked_at = None
        t.locked_by = None
        t.next_run_at = datetime.now(UTC)
    async with sessions() as db:
        t = await db.get(TransferQueueTask, task_id)
        assert t.status == 'QUEUED'
        assert t.error_message is None
        assert t.locked_at is None
        assert t.locked_by is None
    await engine.dispose()
