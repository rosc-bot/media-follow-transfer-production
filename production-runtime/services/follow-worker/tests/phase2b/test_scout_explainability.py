"""Phase 2B tests: Scout explainability, no-share-url ingest, FrameHdr fallback."""

import sqlite3
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.channel import ChannelSetting
from app.models.ingest import ChannelIngestJob
from app.schemas.telegram_source import TelegramSourceMessage
from app.scout.candidate_selector import CandidateSelector
from app.scout.message_search import MessageSearch
from app.scout.scout_service import (
    FINAL_NO_RESOURCE,
    FRAMEHDR_NO_MATCH,
    FRAMEHDR_NOT_ATTEMPTED,
    LOCAL_MATCH,
    LOCAL_NO_MATCH,
    ScoutService,
)


def _resource_db(path, rows):
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE messages(chat_id TEXT, chat_title TEXT, message_id INTEGER, text TEXT, urls TEXT, source_type TEXT, is_forward INTEGER, date TEXT)"
        )
        for row in rows:
            db.execute(
                "INSERT INTO messages VALUES(?,?,?,?,?,?,?,?)",
                row,
            )
        db.commit()


class _Msg:
    def __init__(self, chat_id, message_id, text, urls, chat_title=None):
        self.chat_id = chat_id
        self.message_id = message_id
        self.text = text
        self.urls = urls
        self.chat_title = chat_title


def test_message_search_chinese_and_episode_forms(tmp_path):
    db_path = tmp_path / "r.db"
    _resource_db(db_path, [
        ("3702243011", "资源群", 1, "怪奇物语 S01E03 全集", '["https://pan.guangyapan.com/s/a"]', "telegram_channel", 0, ""),
        ("3702243011", "资源群", 2, "怪奇物语 第3集", '["https://pan.guangyapan.com/s/b"]', "telegram_channel", 0, ""),
        ("3702243011", "资源群", 3, "别的剧 E03", '["https://pan.guangyapan.com/s/c"]', "telegram_channel", 0, ""),
    ])
    search = MessageSearch(str(db_path))
    found = search.search("怪奇物语", "S01E03")
    assert len(found) >= 2
    assert any("第3集" in m.text for m in found)
    found_exx = search.search("别的剧", "S01E03")
    assert len(found_exx) == 1


def test_candidate_selector_explains_rejections(tmp_path):
    candidates = [
        _Msg("1", 10, "剧名 S01E01", ["https://pan.guangyapan.com/s/a"]),
        _Msg("2", 11, "剧名 S01E02", ["https://pan.guangyapan.com/s/b"]),
        _Msg("3", 12, "完全无关 S01E01", ["https://pan.guangyapan.com/s/c"]),
    ]
    explanation = CandidateSelector.explain(candidates, title="剧名", episode_key="S01E01")
    assert explanation["candidate_count"] == 3
    assert explanation["accepted_count"] == 1
    rejected = [e for e in explanation["candidates"] if not e["accepted"]]
    assert len(rejected) == 2
    assert all(e["rejection_reason"] for e in rejected)


def _run_async(coro):
    import asyncio
    return asyncio.new_event_loop().run_until_complete(coro)


@pytest.mark.asyncio
async def test_local_match_stats_and_explainability_async(tmp_path):
    db_path = tmp_path / "r.db"
    _resource_db(db_path, [
        ("3702243011", "资源群", 1, "测试剧 S01E02", '["https://pan.guangyapan.com/s/x"]', "telegram_channel", 0, ""),
    ])
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        results = await ScoutService(MessageSearch(str(db_path))).scout_missing(
            db, tmdb_id=123, title="测试剧", season=1, missing_episodes=["S01E02"],
        )
    assert results[0]["outcome"] == LOCAL_MATCH
    assert results[0]["candidate_count"] >= 1
    assert results[0]["queued"] is True
    stats = ScoutService.summarize(results)
    assert stats["local_hit"] == 1
    assert stats["target_episodes"] == 1
    await engine.dispose()


@pytest.mark.asyncio
async def test_framehdr_fallback_called_on_local_miss(tmp_path):
    db_path = tmp_path / "r.db"
    _resource_db(db_path, [
        ("3702243011", "资源群", 1, "测试剧 S01E02", '["https://pan.guangyapan.com/s/x"]', "telegram_channel", 0, ""),
    ])
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    with patch(
        "app.scout.scout_service.FrameHdrService.search_series",
        new_callable=AsyncMock,
    ) as mock_fh:
        mock_fh.return_value = []
        async with sessions() as db, db.begin():
            results = await ScoutService(MessageSearch(str(db_path))).scout_missing(
                db, tmdb_id=123, title="测试剧", season=1,
                missing_episodes=["S01E02", "S01E03"],
            )
    assert mock_fh.called
    outcomes = {r["outcome"] for r in results}
    local_statuses = {r["local_status"] for r in results}
    fh_statuses = {r["framehdr_status"] for r in results}
    finals = {r["final_status"] for r in results}
    # S01E02 local hit should not trigger framehdr for it; S01E03 miss -> framehdr no match.
    # Phase 2C: local layer must survive the FrameHDR fallback (never zeroed).
    assert LOCAL_MATCH in outcomes or LOCAL_MATCH in local_statuses
    assert LOCAL_NO_MATCH in local_statuses          # S01E03 local miss recorded
    assert FRAMEHDR_NOT_ATTEMPTED in fh_statuses     # S01E02 never reached fallback
    assert FRAMEHDR_NO_MATCH in fh_statuses          # S01E03 fallback attempted, no match
    assert FINAL_NO_RESOURCE in finals
    await engine.dispose()


@pytest.mark.asyncio
async def test_local_hit_does_not_call_framehdr(tmp_path):
    db_path = tmp_path / "r.db"
    _resource_db(db_path, [
        ("3702243011", "资源群", 1, "测试剧 S01E02", '["https://pan.guangyapan.com/s/x"]', "telegram_channel", 0, ""),
    ])
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    with patch(
        "app.scout.scout_service.FrameHdrService.search_series",
        new_callable=AsyncMock,
    ) as mock_fh:
        async with sessions() as db, db.begin():
            await ScoutService(MessageSearch(str(db_path))).scout_missing(
                db, tmdb_id=123, title="测试剧", season=1, missing_episodes=["S01E02"],
            )
    assert not mock_fh.called
    await engine.dispose()


@pytest.mark.asyncio
async def test_ingest_no_share_url_is_skipped_not_failed(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/ingest.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    setting = ChannelSetting(channel_id="test", role="RESOURCE", transfer_mode="AUTO")
    source = TelegramSourceMessage(
        source_type="telegram_channel", channel_id="test", message_id=44,
        text="普通通知消息没有任何分享链接",
    )
    async with sessions() as db, db.begin():
        result = await ChannelIngestService.process_source_message(db, source, channel_setting=setting)
        assert result["status"] == "SKIPPED"
        assert result["skipped"] == "no_share_url"
    await engine.dispose()


@pytest.mark.asyncio
async def test_ingest_unsupported_link_is_skipped(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/ingest.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    setting = ChannelSetting(channel_id="test", role="RESOURCE", transfer_mode="AUTO")
    source = TelegramSourceMessage(
        source_type="telegram_channel", channel_id="test", message_id=45,
        text="内容 https://t.me/random/1",
    )
    async with sessions() as db, db.begin():
        result = await ChannelIngestService.process_source_message(db, source, channel_setting=setting)
        assert result["status"] == "SKIPPED"
        assert result["skipped"] == "unsupported_resource_link"
        job = await db.scalar(select(ChannelIngestJob))
        assert job.error_message == "SKIPPED_UNSUPPORTED_RESOURCE_LINK"
    await engine.dispose()
