import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.ingest import ChannelIngestJob
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.schemas.telegram_source import TelegramSourceMessage


@pytest.mark.asyncio
async def test_same_share_message_can_enqueue_distinct_missing_episodes(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/same_share_multi_episode.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    def source(episode: str) -> TelegramSourceMessage:
        return TelegramSourceMessage(
            source_type="watchlist_scout",
            channel_id="framehdr-channel",
            message_id=9876,
            text="真实剧 https://pan.guangyapan.com/s/share-123",
            urls=["https://pan.guangyapan.com/s/share-123"],
            metadata={
                "tmdb_id": 223564,
                "title": "真实剧",
                "season": 1,
                "episode_keys": [episode],
                "share_url": "https://pan.guangyapan.com/s/share-123",
                "file_names": [f"真实剧.{episode}.mkv"],
            },
        )

    async with sessions() as db, db.begin():
        first = await ChannelIngestService.process_source_message(db, source("S01E08"))
        second = await ChannelIngestService.process_source_message(db, source("S01E09"))
        assert first.get("queued") is True
        assert second.get("queued") is True
        assert second["resource_id"] != first["resource_id"]

    async with sessions() as db:
        resources = list((await db.scalars(select(Resource).order_by(Resource.id))).all())
        tasks = list((await db.scalars(select(TransferQueueTask).order_by(TransferQueueTask.id))).all())
        assert len(resources) == 2
        assert {resource.episode_key for resource in resources} == {"S01E08", "S01E09"}
        assert len(tasks) == 2
        assert {resource.season for resource in resources} == {1}
        assert {tuple(task.payload["episode_keys"]) for task in tasks} == {("S01E08",), ("S01E09",)}

    await engine.dispose()


@pytest.mark.asyncio
async def test_scout_batches_missing_episodes_from_one_share_into_one_task(tmp_path):
    from app.models.resource_candidate import ResourceCandidate
    from app.scout.message_search import ResourceMessage
    from app.scout.scout_service import ScoutService

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/scout_batch.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    share_url = "https://pan.guangyapan.com/s/batch-123"
    message = ResourceMessage(
        chat_id="framehdr-channel",
        message_id=8765,
        text="超超超超超喜欢你的100个女朋友 S01E08 S01E09",
        urls=[share_url],
        chat_title="帧影分享",
    )

    class SharedSearch:
        def search(self, title, episode_key):
            return [message]

    async with sessions() as db, db.begin():
        results = await ScoutService(SharedSearch()).scout_missing(
            db,
            tmdb_id=223564,
            title="超超超超超喜欢你的100个女朋友",
            season=1,
            missing_episodes=["S01E08", "S01E09"],
        )
        assert len(results) == 2
        tasks = list((await db.scalars(select(TransferQueueTask))).all())
        assert len(tasks) == 1
        assert tasks[0].payload["episode_keys"] == ["S01E08", "S01E09"]
        candidates = list((await db.scalars(select(ResourceCandidate))).all())
        assert len(candidates) == 2
        assert {candidate.resource_id for candidate in candidates} == {tasks[0].resource_id}
        assert {candidate.queue_task_id for candidate in candidates} == {tasks[0].id}

    await engine.dispose()


@pytest.mark.asyncio
async def test_scout_identity_evidence_reprocesses_legacy_review_job_and_creates_queued_task(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/scout_identity_reprocess.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    from app.models.channel import ChannelSetting
    setting = ChannelSetting(channel_id="3702243011", role="RESOURCE", enabled=True, transfer_mode="AUTO")
    share_url = "https://pan.guangyapan.com/s/share-304842"
    original = TelegramSourceMessage(
        source_type="telegram_channel", channel_id="3702243011", channel_title="资源群", message_id=32323,
        text="白粉飞：洛城新章 S01E03 https://pan.guangyapan.com/s/share-304842",
        urls=[share_url],
    )
    scout_update = TelegramSourceMessage(
        source_type="watchlist_scout", channel_id="3702243011", channel_title="资源群", message_id=32323,
        text=original.text, urls=[share_url],
        metadata={
            "tmdb_id": 304842, "title": "白粉飞：洛城新章", "season": 1,
            "episode_keys": ["S01E03"], "share_url": share_url,
            "resource_content_hash": "newly-verified-content-hash",
        },
    )

    async with sessions() as db, db.begin():
        first = await ChannelIngestService.process_source_message(db, original, channel_setting=setting)
        assert first["status"] == "NEEDS_REVIEW"
        second = await ChannelIngestService.process_source_message(db, scout_update)
        assert second["queued"] is True
        task = await db.get(TransferQueueTask, second["queue_task_id"])
        assert task.status == "QUEUED"
        assert task.resource_id == second["resource_id"]
        assert task.payload["tmdb_id"] == 304842
    await engine.dispose()


@pytest.mark.asyncio
async def test_changed_candidate_requeues_unfenced_pending_task_after_new_evidence(tmp_path):
    from app.ingest.media_identity import build_identity_key
    from app.transfer.normalization import build_idempotency_key, share_hash

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/pending_new_evidence.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    share_url = "https://pan.guangyapan.com/s/identity-41"
    digest = share_hash(share_url)
    identity = build_identity_key(tmdb_id=41, season=1, episodes=["S01E03"], share_hash=digest)
    source = TelegramSourceMessage(
        source_type="watchlist_scout", channel_id="3702243011", channel_title="资源群", message_id=77,
        text="真实剧 S01E03", urls=[share_url],
        metadata={"tmdb_id": 41, "title": "真实剧", "season": 1, "episode_keys": ["S01E03"],
                  "share_url": share_url, "resource_content_hash": "new-content-hash"},
    )
    async with sessions() as db, db.begin():
        resource = Resource(
            id=41, identity_key=identity, tmdb_id=41, title="真实剧", media_type="tv", season=1,
            episode=3, episode_key="S01E03", cloud_name="guangya", share_url=share_url,
            source_type="watchlist_scout", source_channel_id="3702243011", source_message_id=77,
            status="READY", file_names=[],
        )
        db.add(resource)
        db.add(TransferQueueTask(
            id=41, resource_id=41, idempotency_key=build_idempotency_key(41, "guangya", ["S01E03"]),
            status="PENDING", error_message="old review", payload={
                "provider": "guangya", "tmdb_id": 41, "title": "真实剧", "season": 1,
                "episode_keys": ["S01E03"], "share_url": share_url,
                "preflight_classification": "NEEDS_REVIEW", "preflight_reason": "old reason",
            },
        ))
        db.add(ChannelIngestJob(
            id=41, channel_id="3702243011", message_id=77, source_type="watchlist_scout",
            share_url=share_url, share_hash=digest, status="NEEDS_REVIEW", parsed_data={
                "tmdb_id": 41, "title": "真实剧", "season": 1, "episode_keys": ["S01E03"],
                "resource_content_hash": "old-content-hash",
            }, media_type="tv", tmdb_id=41, title="真实剧", season=1, detected_episodes=["S01E03"],
        ))
        result = await ChannelIngestService.process_source_message(db, source)
        assert result["queued_reactivated"] is True
        assert result["queue_task_id"] == 41
        task = await db.get(TransferQueueTask, 41)
        assert task.status == "QUEUED"
        assert task.error_message is None
        assert "preflight_classification" not in task.payload


@pytest.mark.asyncio
async def test_stale_pending_task_is_not_reported_as_queue_reused(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/stale_pending.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source = TelegramSourceMessage(
        source_type="watchlist_scout",
        channel_id="framehdr-channel",
        message_id=9877,
        text="真实剧 S01E08 https://pan.guangyapan.com/s/share-456",
        urls=["https://pan.guangyapan.com/s/share-456"],
        metadata={"tmdb_id": 223564, "title": "真实剧", "season": 1,
                  "episode_keys": ["S01E08"], "share_url": "https://pan.guangyapan.com/s/share-456"},
    )
    async with sessions() as db, db.begin():
        first = await ChannelIngestService.process_source_message(db, source)
        task = await db.scalar(select(TransferQueueTask))
        job = await db.scalar(select(ChannelIngestJob))
        assert task is not None and job is not None
        task.status = "PENDING"
        job.transfer_status = "QUEUED"  # stale message-level cache
        second = await ChannelIngestService.process_source_message(db, source)
        assert first["queued"] is True
        assert second["queue_reused"] is False
        assert second.get("queued") is not True
    await engine.dispose()
