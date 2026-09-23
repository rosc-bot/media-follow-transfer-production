import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.channel import ChannelSetting
from app.models.ingest import ChannelIngestJob, ChannelIngestMessage
from app.models.resource import Resource, ResourceStatus
from app.schemas.telegram_source import TelegramSourceMessage


def source(*, message_id: int, url: str) -> TelegramSourceMessage:
    return TelegramSourceMessage(
        source_type="manual_forward",
        channel_id="dedup-test",
        message_id=message_id,
        text=f"同一候选 S01E02 {url}",
        is_forward=True,
        metadata={"tmdb_id": 333, "title": "同一候选", "file_names": ["同一候选.S01E02.mkv"]},
    )


@pytest.mark.asyncio
async def test_failed_resource_does_not_block_different_source_with_same_identity(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/dedup.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    setting = ChannelSetting(channel_id="dedup-test", role="MANUAL_INGEST", accept_forward=True, transfer_mode="AUTO")

    async with sessions() as db, db.begin():
        first = await ChannelIngestService.process_source_message(db, source(message_id=1, url="https://pan.guangyapan.com/s/same"), channel_setting=setting)
    async with sessions() as db, db.begin():
        resource = await db.get(Resource, first["resource_id"])
        resource.status = ResourceStatus.FAILED
    async with sessions() as db, db.begin():
        replacement = await ChannelIngestService.process_source_message(db, source(message_id=2, url="https://pan.guangyapan.com/s/same"), channel_setting=setting)
        assert replacement.get("deduplicated") is not True

    async with sessions() as db:
        resources = list((await db.scalars(select(Resource).order_by(Resource.id))).all())
        assert [resource.status for resource in resources] == [ResourceStatus.FAILED, ResourceStatus.READY]
        assert resources[0].identity_key == resources[1].identity_key

    await engine.dispose()


@pytest.mark.asyncio
async def test_same_message_stays_deduplicated_even_if_its_resource_failed(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/message.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    setting = ChannelSetting(channel_id="dedup-test", role="MANUAL_INGEST", accept_forward=True, transfer_mode="AUTO")

    async with sessions() as db, db.begin():
        first = await ChannelIngestService.process_source_message(db, source(message_id=3, url="https://pan.guangyapan.com/s/same"), channel_setting=setting)
    async with sessions() as db, db.begin():
        resource = await db.get(Resource, first["resource_id"])
        resource.status = ResourceStatus.FAILED
    async with sessions() as db, db.begin():
        duplicate = await ChannelIngestService.process_source_message(db, source(message_id=3, url="https://pan.guangyapan.com/s/same"), channel_setting=setting)
        assert duplicate["deduplicated"] is True

    async with sessions() as db:
        assert len(list((await db.scalars(select(Resource))).all())) == 1
        assert len(list((await db.scalars(select(ChannelIngestMessage))).all())) == 1
        assert len(list((await db.scalars(select(ChannelIngestJob))).all())) == 1
    await engine.dispose()
