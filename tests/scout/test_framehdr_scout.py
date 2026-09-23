from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.database import Base
from app.scout.message_search import MessageSearch
from app.scout.scout_service import ScoutService


@pytest.mark.asyncio
async def test_scout_falls_back_to_framehdr_when_messages_empty(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/test.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    # Empty search
    class EmptySearch(MessageSearch):
        def __init__(self):
            pass

        def search(self, title: str, episode_key: str):
            return []

    mock_fh_results = [
        {
            'msg_id': 800012345,
            'chat_title': '帧影·测试分享',
            'title': '追新测试剧',
            'season': 1,
            'provider': 'guangya',
            'url': 'https://pan.guangyapan.com/s/fh123?code=9999',
            'matched_episodes': [1],
            'snippet': '[帧影分享] 追新测试剧 S01E01 4K',
        }
    ]

    scout = ScoutService(EmptySearch())
    with patch('app.scout.framehdr.FrameHdrService.search_series', new_callable=AsyncMock) as mock_search:
        mock_search.return_value = mock_fh_results
        async with sessions() as db:
            results = await scout.scout_missing(
                db,
                tmdb_id=8888,
                title='追新测试剧',
                season=1,
                missing_episodes=['S01E01'],
            )
            assert len(results) == 1
            assert results[0]['queued'] is True
            assert mock_search.called


@pytest.mark.asyncio
async def test_scout_bounds_framehdr_episode_batches(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/batch.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    class EmptySearch(MessageSearch):
        def __init__(self):
            pass

        def search(self, title: str, episode_key: str):
            return []

    async def framehdr_side_effect(*, episodes, **_kwargs):
        return [{
            'msg_id': 900000000 + episodes[0],
            'chat_title': '帧影·批次测试',
            'url': f'https://pan.guangyapan.com/s/batch{episodes[0]}',
            'matched_episodes': list(episodes),
            'snippet': '批次测试',
        }]

    scout = ScoutService(EmptySearch())
    missing = [f'S01E{episode:02d}' for episode in range(1, 18)]
    with patch('app.scout.framehdr.FrameHdrService.search_series', new_callable=AsyncMock) as mock_search:
        mock_search.side_effect = framehdr_side_effect
        async with sessions() as db:
            results = await scout.scout_missing(
                db,
                tmdb_id=9999,
                title='批次测试剧',
                season=1,
                missing_episodes=missing,
            )
    requested = [call.kwargs['episodes'] for call in mock_search.await_args_list]
    assert requested == [list(range(1, 9)), list(range(9, 17)), [17]]
    assert all(len(batch) <= 8 for batch in requested)
    assert len(results) == 17
    await engine.dispose()


@pytest.mark.asyncio
async def test_framehdr_full_share_queues_all_missing_episodes_as_one_task(tmp_path):
    from sqlalchemy import select

    from app.models.resource_candidate import ResourceCandidate
    from app.models.transfer import TransferQueueTask

    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/framehdr_full_share.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    class EmptySearch(MessageSearch):
        def __init__(self):
            pass

        def search(self, title: str, episode_key: str):
            return []

    share_url = 'https://pan.guangyapan.com/s/full-season'
    scout = ScoutService(EmptySearch())
    with patch('app.scout.framehdr.FrameHdrService.search_series', new_callable=AsyncMock) as mock_search:
        mock_search.return_value = [{
            'msg_id': 990000001,
            'chat_title': '帧影·全集',
            'url': share_url,
            'matched_episodes': [8, 9, 10],
            'snippet': '超超超超超喜欢你的100个女朋友 S01E01-E12',
        }]
        async with sessions() as db, db.begin():
            results = await scout.scout_missing(
                db,
                tmdb_id=223564,
                title='超超超超超喜欢你的100个女朋友',
                season=1,
                missing_episodes=['S01E08', 'S01E09', 'S01E10'],
            )
            tasks = list((await db.scalars(select(TransferQueueTask))).all())
            assert len(results) == 3
            assert len(tasks) == 1
            assert tasks[0].payload['episode_keys'] == ['S01E08', 'S01E09', 'S01E10']
            candidates = list((await db.scalars(select(ResourceCandidate))).all())
            assert {candidate.episode_key for candidate in candidates} == {'S01E08', 'S01E09', 'S01E10'}
            assert {candidate.resource_id for candidate in candidates} == {tasks[0].resource_id}
            assert {candidate.queue_task_id for candidate in candidates} == {tasks[0].id}

    await engine.dispose()
