import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.follow.completion_promotion_service import CompletionPromotionService
from app.models.cloud import CloudConfig, CloudDiskInventory
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist


@pytest.mark.asyncio
@pytest.mark.parametrize("include_second_season", [False, True])
async def test_completed_ongoing_series_is_enqueued_for_one_safe_promotion(tmp_path, monkeypatch, include_second_season):
    async def fake_metadata(resource, *, force_refresh=False):
        assert force_refresh is True
        return {
            "id": resource.tmdb_id,
            "media_type": "tv",
            "origin_country": ["CN"],
            "original_language": "zh",
            "genres": [{"id": 18, "name": "Drama"}],
            "metadata_complete": True,
            "status": "Ended",
            "seasons": [
                {"season_number": 1, "episode_count": 2},
                {"season_number": 2, "episode_count": 1},
            ],
        }

    monkeypatch.setattr(CompletionPromotionService, "_tmdb_metadata", staticmethod(fake_metadata))
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/promotion.db')
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        db.add(CloudConfig(
            name='guangya', auth_ref='test-auth', enabled=True,
            target_folder_id='completed-root', ongoing_target_folder_id='ongoing-root',
        ))
        db.add(SeriesWatchlist(
            tmdb_id=99, title='迁移剧', year=2025, season=1, status='FOLLOWING',
            tmdb_series_status='Ended', total_episodes=2,
            collected_episodes=['S01E01', 'S01E02'],
            remote_series_folder_id='ongoing-series-id', remote_destination_kind='ongoing',
        ))
        db.add(Resource(
            id=99, identity_key='promotion-resource', tmdb_id=99, title='迁移剧', year=2025,
            media_type='tv', season=1, episode=2, episode_key='S01E02',
            cloud_name='guangya', share_url='https://pan.guangyapan.com/s/promotion',
            source_type='watchlist_scout', file_names=['S01E02.mkv'],
        ))
        db.add_all([
            CloudDiskInventory(title='迁移剧', clean_title='迁移剧', tmdb_id=99, season=1, episode=1, file_name='S01E01.mkv'),
            CloudDiskInventory(title='迁移剧', clean_title='迁移剧', tmdb_id=99, season=1, episode=2, file_name='S01E02.mkv'),
        ])
        if include_second_season:
            db.add(SeriesWatchlist(
                tmdb_id=99, title='迁移剧', year=2025, season=2, status='FOLLOWING',
                tmdb_series_status='Ended', total_episodes=1,
                collected_episodes=['S02E01'],
                remote_series_folder_id='ongoing-series-id', remote_destination_kind='ongoing',
            ))
            db.add(CloudDiskInventory(
                title='迁移剧', clean_title='迁移剧', tmdb_id=99, season=2,
                episode=1, file_name='S02E01.mkv',
            ))

    cloud_keys = {1: {"S01E01", "S01E02"}}
    if include_second_season:
        cloud_keys[2] = {"S02E01"}
    async with sessions() as db, db.begin():
        count = await CompletionPromotionService.enqueue_ready_promotions(
            db, cloud_episode_keys_by_season=cloud_keys,
        )
        assert count == (1 if include_second_season else 0)
        assert await CompletionPromotionService.enqueue_ready_promotions(
            db, cloud_episode_keys_by_season=cloud_keys,
        ) == 0

    async with sessions() as db:
        task = await db.scalar(select(TransferQueueTask))
        if include_second_season:
            assert task is not None
            assert task.payload['operation'] == 'promote'
            assert task.payload['promotion_source_series_folder_id'] == 'ongoing-series-id'
            assert task.payload['target_folder_id'] == 'completed-root'
            assert task.payload['series_folder_name'] == '迁移剧 (2025) {tmdbid-99}'
            assert task.payload['season_folder_name'] == 'S01'
            assert task.payload['promotion_gate']['total_expected'] == 3
            assert task.payload['promotion_expected_files_by_season'] == {
                'S01': ['S01E01.mkv', 'S01E02.mkv'], 'S02': ['S02E01.mkv'],
            }
        else:
            assert task is None
    await engine.dispose()


def test_returning_series_cannot_promote_from_complete_counts():
    from app.follow.promotion import evaluate_promotion

    decision = evaluate_promotion(
        tmdb_id=99, title="未完结剧", series_status="Returning Series",
        ongoing_root="ongoing", completed_root="completed",
        seasons=[{
            "season": 1, "total_expected": 2, "collected_count": 2,
            "inventory_count": 2, "cloud_count": 2,
            "series_status": "Returning Series",
        }],
    )
    assert decision.decision == "SERIES_NOT_ENDED"
    assert decision.reason == "AUTHORITATIVE_STATUS_NOT_ENDED"
