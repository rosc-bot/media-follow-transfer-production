"""Regression for quality upgrades and completed-series inventory fallback."""
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.cloud import CloudDiskInventory
from app.models.watchlist import SeriesWatchlist
from app.schemas.telegram_source import TelegramSourceMessage
from app.transfer.quality_rank import extract_quality_score


def test_resource_quality_score_keeps_4k_webdl_above_1080p_remux():
    assert extract_quality_score('Show 4K REMUX') > extract_quality_score('Show 4K WEB-DL')
    assert extract_quality_score('Show 4K WEB-DL') > extract_quality_score('Show 1080p REMUX')


@pytest.mark.asyncio
async def test_completed_series_with_only_inventory_deduplicates_without_name_error(monkeypatch):
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        monkeypatch.setattr(ChannelIngestService, '_search_tmdb', AsyncMock(return_value=(7, 'tv')))
        async with sessions() as db, db.begin():
            db.add(SeriesWatchlist(tmdb_id=7, title='Inventory only', season=1, status='COMPLETED', collected_episodes=['S01E01']))
            db.add(CloudDiskInventory(tmdb_id=7, title='Inventory only', clean_title='Inventory only', season=1, episode=1, file_name='Show.S01E01.2160p.REMUX.mkv'))
        async with sessions() as db, db.begin():
            result = await ChannelIngestService.process_source_message(db, TelegramSourceMessage(
                source_type='watchlist_scout', channel_id='unit-inventory', message_id='1', is_forward=True,
                text='Inventory only S01E01 1080p WEB-DL https://pan.guangyapan.com/s/unit-share',
                metadata={'tmdb_id':7,'title':'Inventory only','season':1,'episode_keys':['S01E01'],'media_type':'tv'},
            ))
            assert 'skipped' in result, result
            assert result['skipped'] == 'series_already_completed'
            assert result['deduplicated'] is True
    finally:
        await engine.dispose()
