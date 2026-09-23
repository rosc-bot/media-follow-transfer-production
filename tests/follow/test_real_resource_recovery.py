import json
import sqlite3

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.follow.missing_episode_service import MissingEpisodeService
from app.models.ingest import ChannelIngestJob
from app.models.resource import Resource
from app.models.resource_candidate import ResourceCandidate
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.scout.message_search import MessageSearch
from app.scout.scout_service import ScoutService
from app.transfer.normalization import share_hash


@pytest.mark.asyncio
async def test_realistic_old_resource_candidate_flows_to_new_queued_task_after_title_search(tmp_path):
    resource_db = tmp_path / "resource_messages.db"
    url = "https://pan.guangyapan.com/s/update-304842"
    caption = "白粉飞：洛城新章 S01E03 2160p"
    content_hash = "edited-message-content-hash"
    with sqlite3.connect(resource_db) as db:
        db.execute(
            """CREATE TABLE messages(
                id INTEGER PRIMARY KEY, chat_id TEXT, chat_title TEXT, message_id INTEGER,
                text TEXT, caption TEXT, urls TEXT, source_type TEXT, is_forward INTEGER,
                date TEXT, content_hash TEXT, updated_at TEXT
            )"""
        )
        db.executemany(
            "INSERT INTO messages(chat_id,chat_title,message_id,text,caption,urls,source_type,is_forward,date,content_hash,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("4429917555", "资源频道", index, "无关新资源", "", "[]", "telegram_channel", 0,
                 f"2026-09-24T00:{index % 60:02d}:00Z", f"noise-{index}", f"2026-09-24T00:{index % 60:02d}:00Z")
                for index in range(1, 201)
            ]
            + [("3702243011", "光鸭云盘资源分享群", 32323, "", caption, json.dumps([url]),
                "telegram_channel", 0, "2026-09-23T09:21:06Z", content_hash, "2026-09-23T16:00:00Z")],
        )

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        watchlist = SeriesWatchlist(
            tmdb_id=304842, title="白粉飞：洛城新章", year=2026, media_type="tv", season=1,
            status="FOLLOWING", follow_mode="LATEST", total_episodes=8, last_aired_episode=3,
            collected_episodes=["S01E01", "S01E02"], tmdb_series_status="Returning Series",
        )
        db.add(watchlist)
        db.add(ChannelIngestJob(
            channel_id="3702243011", message_id=32323, source_type="telegram_channel",
            share_url=url, share_hash=share_hash(url), status="NEEDS_REVIEW", media_type="tv",
            tmdb_id=None, title=caption, season=1, detected_episodes=["S01E03"],
            parsed_data={"episode_keys": ["S01E03"], "resource_content_hash": None},
            error_message="missing tmdb_id or season/episode identity",
        ))

    async with sessions() as db, db.begin():
        missing = MissingEpisodeService.missing_for_watchlist(
            watchlist, follow_mode="LATEST", recent_limit=30,
        )
        assert missing == ["S01E03"]
        results = await ScoutService(MessageSearch(str(resource_db))).scout_missing(
            db,
            tmdb_id=304842,
            title="白粉飞：洛城新章",
            season=1,
            missing_episodes=missing,
            year=2026,
        )
        assert results[0]["queued"] is True
        assert results[0]["final_status"] == "QUEUED"
        task = await db.scalar(select(TransferQueueTask).where(TransferQueueTask.status == "QUEUED"))
        resource = await db.get(Resource, task.resource_id)
        candidate = await db.scalar(select(ResourceCandidate).where(
            ResourceCandidate.tmdb_id == 304842,
            ResourceCandidate.episode_key == "S01E03",
            ResourceCandidate.source_message_id == 32323,
        ))
        job = await db.scalar(select(ChannelIngestJob).where(
            ChannelIngestJob.channel_id == "3702243011",
            ChannelIngestJob.message_id == 32323,
        ))
        assert task is not None
        assert task.status == "QUEUED"
        assert resource is not None and resource.tmdb_id == 304842 and resource.episode_key == "S01E03"
        assert candidate is not None and candidate.resource_id == resource.id and candidate.queue_task_id == task.id
        assert job is not None and job.tmdb_id == 304842 and job.transfer_status == "QUEUED"

    await engine.dispose()
