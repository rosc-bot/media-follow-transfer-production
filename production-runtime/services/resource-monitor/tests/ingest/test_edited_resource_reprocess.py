import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.schemas.telegram_source import TelegramSourceMessage


@pytest.mark.asyncio
async def test_edited_resource_message_with_new_episode_reprocesses_into_queue_task(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/edited_resource_message.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    share_url = "https://pan.guangyapan.com/s/share-304842"
    original = TelegramSourceMessage(
        source_type="watchlist_scout",
        channel_id="3702243011",
        channel_title="资源群",
        message_id=32323,
        text=f"白粉飞：洛城新章 S01E03 {share_url}",
        caption="白粉飞：洛城新章 S01E03",
        urls=[share_url],
        metadata={"tmdb_id": 304842, "title": "白粉飞：洛城新章", "season": 1,
                  "episode_keys": ["S01E03"], "share_url": share_url,
                  "resource_content_hash": "hash-before-edit"},
    )
    edited = original.model_copy(update={
        "text": f"白粉飞：洛城新章 S01E03 S01E04 {share_url}",
        "caption": "白粉飞：洛城新章 S01E03 S01E04",
        "metadata": {**original.metadata, "episode_keys": ["S01E03", "S01E04"],
                     "resource_content_hash": "hash-after-edit"},
    })

    async with sessions() as db, db.begin():
        first = await ChannelIngestService.process_source_message(db, original)
        edited_result = await ChannelIngestService.process_source_message(db, edited)
        assert first["queued"] is True
        assert edited_result["queued"] is True
        assert edited_result["resource_id"] != first["resource_id"]

    async with sessions() as db:
        resources = list((await db.scalars(select(Resource).order_by(Resource.id))).all())
        tasks = list((await db.scalars(select(TransferQueueTask).order_by(TransferQueueTask.id))).all())
        assert {resource.episode_key for resource in resources} == {"S01E03", "S01E04"}
        assert len(tasks) == 2
        assert {tuple(task.payload["episode_keys"]) for task in tasks} == {("S01E03",), ("S01E04",)}

    await engine.dispose()
