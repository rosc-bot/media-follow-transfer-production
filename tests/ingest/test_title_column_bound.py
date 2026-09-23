import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.ingest import ChannelIngestJob
from app.models.resource import Resource
from app.schemas.telegram_source import TelegramSourceMessage


@pytest.mark.asyncio
async def test_long_source_title_is_bounded_for_varchar_columns(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/bounded_title.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    long_title = "剧" * 520
    source = TelegramSourceMessage(
        source_type="watchlist_scout",
        channel_id="resource-channel",
        message_id=123,
        text="S01E01",
        urls=["https://pan.guangyapan.com/s/title-limit"],
        metadata={
            "tmdb_id": 998877,
            "title": long_title,
            "season": 1,
            "episode_keys": ["S01E01"],
            "share_url": "https://pan.guangyapan.com/s/title-limit",
        },
    )

    async with sessions() as db, db.begin():
        await ChannelIngestService.process_source_message(db, source)
        job = await db.scalar(select(ChannelIngestJob))
        resource = await db.scalar(select(Resource))
        assert job is not None and len(job.title) == 512
        assert resource is not None and len(resource.title) == 512

    await engine.dispose()
