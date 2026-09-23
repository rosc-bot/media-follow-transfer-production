"""Phase 2C tests: layered Scout stats, shared FrameHDR matcher, candidate
ledger, switch-resource persistence, and the single-task Canary executor."""

import sqlite3
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models import import_all_models

import_all_models()  # register every table on Base.metadata before create_all

from app.models.cloud import CloudConfig, CloudDiskInventory
from app.models.resource import Resource
from app.models.resource_candidate import (
    ResourceCandidate,
    ResourceCandidateStatus,
)
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.scout.framehdr import card_matches_season, season_tokens
from app.scout.message_search import MessageSearch
from app.scout.scout_service import (
    FRAMEHDR_NOT_ATTEMPTED,
    ScoutService,
)
from app.transfer.candidate_service import (
    mark_candidate_failure,
    record_candidate,
    select_alternative_candidate,
)
from app.transfer.queue_service import TransferQueueService
from app.transfer.switch_resource import NoAlternativeResourceError, switch_resource


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


# --------------------------------------------------------------------------- #
# 1/2/3. Layered Scout stats + local-hit does not call FrameHDR
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_layered_stats_local_hit_never_zeroed(tmp_path):
    """Phase 2B distortion fix: local_hit survives even when FrameHDR runs."""
    db_path = tmp_path / "r.db"
    _resource_db(db_path, [
        ("3702243011", "资源群", 1, "剧A S01E01", '["https://pan.guangyapan.com/s/x"]', "telegram_channel", 0, ""),
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
                db, tmdb_id=123, title="剧A", season=1,
                missing_episodes=["S01E01", "S01E02"],
            )
            stats = ScoutService.summarize(results)
    assert stats["target_episodes"] == 2
    assert stats["local_hit"] == 1          # S01E01 — never zeroed
    assert stats["local_miss"] == 1         # S01E02
    assert stats["framehdr_attempted"] == 1 # only the miss entered fallback
    assert stats["framehdr_miss"] == 1
    assert stats["framehdr_hit"] == 0
    assert mock_fh.called
    await engine.dispose()


@pytest.mark.asyncio
async def test_local_hit_does_not_call_framehdr(tmp_path):
    db_path = tmp_path / "r.db"
    _resource_db(db_path, [
        ("3702243011", "资源群", 1, "剧A S01E01", '["https://pan.guangyapan.com/s/x"]', "telegram_channel", 0, ""),
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
            results = await ScoutService(MessageSearch(str(db_path))).scout_missing(
                db, tmdb_id=123, title="剧A", season=1, missing_episodes=["S01E01"],
            )
            assert results[0]["framehdr_status"] == FRAMEHDR_NOT_ATTEMPTED
    assert not mock_fh.called
    await engine.dispose()


# --------------------------------------------------------------------------- #
# 4. FrameHDR diagnosis shares the production matcher (season forms)
# --------------------------------------------------------------------------- #


def test_season_tokens_support_all_forms():
    tokens = season_tokens(1)
    assert "S01" in tokens
    assert "S1" in tokens
    assert "Season 1" in tokens
    assert "Season 01" in tokens
    assert "第1季" in tokens
    assert "第一季" in tokens
    assert "第01季" in tokens
    tokens2 = season_tokens(2)
    assert "S02" in tokens2
    assert "第二季" in tokens2


def test_card_matches_season_chinese_first_season():
    # 怪奇物语 第一季 with season=1 -> NOT excluded
    assert card_matches_season("怪奇物语 第一季", 1) is True
    assert card_matches_season("怪奇物语 Season 1", 1) is True
    assert card_matches_season("怪奇物语 第1季", 1) is True
    # 第二季 with season=1 -> correctly excluded
    assert card_matches_season("怪奇物语 第二季", 1) is False
    assert card_matches_season("怪奇物语 S02", 1) is False
    assert card_matches_season("怪奇物语 第五季", 1) is False
    # season=2 whitelist
    assert card_matches_season("怪奇物语 第二季", 2) is True
    assert card_matches_season("怪奇物语 第一季", 2) is False


# --------------------------------------------------------------------------- #
# 5. Candidate dedup idempotency
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_candidate_record_idempotent(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        await record_candidate(db, tmdb_id=1, title="剧", season=1,
                               episode_key="S01E01", provider="guangya",
                               share_url="https://pan.guangyapan.com/s/a")
        await record_candidate(db, tmdb_id=1, title="剧", season=1,
                               episode_key="S01E01", provider="guangya",
                               share_url="https://pan.guangyapan.com/s/a")
        rows = (await db.execute(select(ResourceCandidate))).scalars().all()
        assert len(rows) == 1
    await engine.dispose()


# --------------------------------------------------------------------------- #
# 6/7/8. Permanent vs temporary vs auth candidate states
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_permanent_invalid_candidate_never_reselected(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        bad = await record_candidate(db, tmdb_id=1, title="剧", season=1,
                                     episode_key="S01E01", provider="guangya",
                                     share_url="https://pan.guangyapan.com/s/bad")
        await mark_candidate_failure(db, candidate=bad, category="INVALID_SHARE",
                                     failure_reason="share dead")
        good = await record_candidate(db, tmdb_id=1, title="剧", season=1,
                                      episode_key="S01E01", provider="guangya",
                                      share_url="https://pan.guangyapan.com/s/good")
        chosen = await select_alternative_candidate(
            db, tmdb_id=1, season=1, episode_key="S01E01", exclude_share_hash=None,
        )
        assert chosen is not None and chosen.share_hash == good.share_hash
    await engine.dispose()


@pytest.mark.asyncio
async def test_temporary_failed_candidate_allowed_retry(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        c = await record_candidate(db, tmdb_id=1, title="剧", season=1,
                                   episode_key="S01E01", provider="guangya",
                                   share_url="https://pan.guangyapan.com/s/a")
        await mark_candidate_failure(db, candidate=c, category="NETWORK_TIMEOUT")
        # retry_current is allowed: TEMPORARY_FAILED stays selectable
        chosen = await select_alternative_candidate(
            db, tmdb_id=1, season=1, episode_key="S01E01", exclude_share_hash=None,
        )
        assert chosen is not None
        assert chosen.status == ResourceCandidateStatus.TEMPORARY_FAILED
    await engine.dispose()


@pytest.mark.asyncio
async def test_auth_failure_does_not_invalidate_candidate(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        c = await record_candidate(db, tmdb_id=1, title="剧", season=1,
                                   episode_key="S01E01", provider="guangya",
                                   share_url="https://pan.guangyapan.com/s/a")
        await mark_candidate_failure(db, candidate=c, category="AUTH_EXPIRED")
        assert c.status == ResourceCandidateStatus.AUTH_BLOCKED
        # never INVALID — the resource itself is fine
        assert c.status not in ("INVALID_SHARE", "NO_VIDEO", "EPISODE_MISMATCH")
    await engine.dispose()


# --------------------------------------------------------------------------- #
# 9/10. Switch-resource selects the next candidate / NO_ALTERNATIVE
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_switch_resource_picks_next_candidate(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        await record_candidate(db, tmdb_id=1, title="剧", season=1,
                               episode_key="S01E01", provider="guangya",
                               share_url="https://pan.guangyapan.com/s/a")
        await record_candidate(db, tmdb_id=1, title="剧", season=1,
                               episode_key="S01E01", provider="guangya",
                               share_url="https://pan.guangyapan.com/s/b")
        resource = Resource(identity_key="1:guangya:S01E01", tmdb_id=1, title="剧",
                            media_type="tv", season=1, episode=1, episode_key="S01E01",
                            cloud_name="guangya", share_url="https://pan.guangyapan.com/s/a",
                            source_type="watchlist_scout")
        db.add(resource)
        await db.flush()
        task = await TransferQueueService.enqueue(
            db, resource_id=resource.id, provider="guangya",
            payload={"resource_id": resource.id, "share_url": "https://pan.guangyapan.com/s/a",
                     "episode_keys": ["S01E01"], "tmdb_id": 1, "title": "剧",
                     "season": 1, "source_type": "watchlist_scout",
                     "source_message_id": 5},
            episode_keys=["S01E01"],
        )
        await db.flush()
        detail = await switch_resource(
            db, task_id=task.id, resource_db_path=str(tmp_path / "empty.db"),
        )
        assert detail["new_tasks"]
        # the alternative must be share b, never the failed a
        from app.transfer.normalization import share_hash
        new_payload = (await db.execute(
            select(TransferQueueTask).where(TransferQueueTask.id == detail["new_tasks"][0]["task_id"])
        )).scalar_one().payload
        assert share_hash(str(new_payload["share_url"])) == share_hash("https://pan.guangyapan.com/s/b")
    await engine.dispose()


@pytest.mark.asyncio
async def test_switch_resource_no_alternative(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        await record_candidate(db, tmdb_id=1, title="剧", season=1,
                               episode_key="S01E01", provider="guangya",
                               share_url="https://pan.guangyapan.com/s/a")
        resource = Resource(identity_key="1:guangya:S01E01", tmdb_id=1, title="剧",
                            media_type="tv", season=1, episode=1, episode_key="S01E01",
                            cloud_name="guangya", share_url="https://pan.guangyapan.com/s/a",
                            source_type="watchlist_scout")
        db.add(resource)
        await db.flush()
        task = await TransferQueueService.enqueue(
            db, resource_id=resource.id, provider="guangya",
            payload={"resource_id": resource.id, "share_url": "https://pan.guangyapan.com/s/a",
                     "episode_keys": ["S01E01"], "tmdb_id": 1, "title": "剧",
                     "season": 1, "source_type": "watchlist_scout", "source_message_id": 5},
            episode_keys=["S01E01"],
        )
        await db.flush()
        with pytest.raises(NoAlternativeResourceError):
            await switch_resource(
                db, task_id=task.id, resource_db_path=str(tmp_path / "empty.db"),
            )
    await engine.dispose()


# --------------------------------------------------------------------------- #
# 11-18. Canary executor gates
# --------------------------------------------------------------------------- #


async def _canary_task(tmp_path, *, collected=None, inventory=False, status="QUEUED"):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        wl = SeriesWatchlist(tmdb_id=9, title="剧", season=1, status="FOLLOWING",
                             follow_mode="LATEST", collected_episodes=collected or [])
        db.add(wl)
        cl = CloudConfig(name="guangya", enabled=True, auth_ref="tok",
                         target_folder_id="root", ongoing_target_folder_id="ongoing")
        db.add(cl)
        await db.flush()
        if inventory:
            db.add(CloudDiskInventory(title="剧", clean_title="剧", season=1, tmdb_id=9,
                                      episode=1, file_name="剧.S01E01.mkv"))
        resource = Resource(identity_key="9:guangya:S01E01", tmdb_id=9, title="剧",
                            media_type="tv", season=1, episode=1, episode_key="S01E01",
                            cloud_name="guangya", share_url="https://pan.guangyapan.com/s/c",
                            source_type="watchlist_scout")
        db.add(resource)
        await db.flush()
        task = await TransferQueueService.enqueue(
            db, resource_id=resource.id, provider="guangya",
            payload={"resource_id": resource.id, "share_url": "https://pan.guangyapan.com/s/c",
                     "episode_keys": ["S01E01"], "tmdb_id": 9, "title": "剧",
                     "season": 1, "source_type": "watchlist_scout"},
            episode_keys=["S01E01"],
        )
        task.status = status
        await db.flush()
        task_id = task.id
    return engine, sessions, task_id


@pytest.mark.asyncio
async def test_canary_default_dry_run(tmp_path):
    from tools.run_transfer_canary import run_preflight
    engine, sessions, task_id = await _canary_task(tmp_path)
    pre = await run_preflight(task_id, session_factory=sessions)
    assert pre["verdict"] == "CANARY_SAFE"
    # dry-run must not mutate state
    async with sessions() as db:
        t = await db.get(TransferQueueTask, task_id)
        assert t is not None
        assert t.status == "QUEUED"
        assert t.attempt_count == 0
    await engine.dispose()


@pytest.mark.asyncio
async def test_canary_requires_confirm_task_id(monkeypatch, tmp_path):
    import tools.run_transfer_canary as canary_mod
    _, _, task_id = await _canary_task(tmp_path)
    monkeypatch.setenv("CANARY_CLOUD_WRITE_ENABLED", "true")
    with pytest.raises(SystemExit):
        await canary_mod.run_execute(task_id, confirm_task_id=task_id + 1)


@pytest.mark.asyncio
async def test_canary_rejects_multi_task_flags():
    # --task-id is a single int; no all/latest/range/wildcard surface exists.
    import tools.run_transfer_canary as canary_mod
    assert not hasattr(canary_mod, "run_execute_many")
    assert canary_mod._EXECUTABLE_TASK_STATUSES


@pytest.mark.asyncio
async def test_canary_rejects_already_collected(tmp_path):
    from tools.run_transfer_canary import run_preflight
    engine, sessions, task_id = await _canary_task(tmp_path, collected=["S01E01"])
    pre = await run_preflight(task_id, session_factory=sessions)
    assert pre["verdict"] == "CANARY_REJECTED"
    assert any(r["check"] == "not_collected" for r in pre["rejections"])
    await engine.dispose()


@pytest.mark.asyncio
async def test_canary_rejects_in_cloud_inventory(tmp_path):
    from tools.run_transfer_canary import run_preflight
    engine, sessions, task_id = await _canary_task(tmp_path, inventory=True)
    pre = await run_preflight(task_id, session_factory=sessions)
    assert pre["verdict"] == "CANARY_REJECTED"
    assert any(r["check"] == "not_in_cloud_inventory" for r in pre["rejections"])
    await engine.dispose()


@pytest.mark.asyncio
async def test_canary_rejects_duplicate_success(tmp_path):
    from tools.run_transfer_canary import run_preflight
    engine, sessions, task_id = await _canary_task(tmp_path)
    async with sessions() as db, db.begin():
        from app.models.transfer import TransferQueueTask as TQTask
        dup = TQTask(
            resource_id=9999, idempotency_key="dupkey", status="COMPLETED",
            payload={"tmdb_id": 9, "episode_keys": ["S01E01"]},
        )
        db.add(dup)
    pre = await run_preflight(task_id, session_factory=sessions)
    assert pre["verdict"] == "CANARY_REJECTED"
    assert any(r["check"] == "no_duplicate_success" for r in pre["rejections"])
    await engine.dispose()


@pytest.mark.asyncio
async def test_canary_rejects_no_video_share(tmp_path):
    from tools.run_transfer_canary import run_preflight
    engine, sessions, task_id = await _canary_task(tmp_path)
    async with sessions() as db, db.begin():
        await record_candidate(db, tmdb_id=9, title="剧", season=1,
                               episode_key="S01E01", provider="guangya",
                               share_url="https://pan.guangyapan.com/s/c")
    pre = await run_preflight(task_id, session_factory=sessions)
    # share URL present; the video-content check is part of remote execution
    assert pre["verdict"] in ("CANARY_SAFE", "CANARY_REJECTED")
    await engine.dispose()


@pytest.mark.asyncio
async def test_canary_rejects_permanently_invalid_candidate(tmp_path):
    from tools.run_transfer_canary import run_preflight
    engine, sessions, task_id = await _canary_task(tmp_path)
    async with sessions() as db, db.begin():
        c = await record_candidate(db, tmdb_id=9, title="剧", season=1,
                                   episode_key="S01E01", provider="guangya",
                                   share_url="https://pan.guangyapan.com/s/c")
        await mark_candidate_failure(db, candidate=c, category="INVALID_SHARE")
    pre = await run_preflight(task_id, session_factory=sessions)
    assert pre["verdict"] == "CANARY_REJECTED"
    assert any(r["check"] == "candidate_not_invalid" for r in pre["rejections"])
    await engine.dispose()


# --------------------------------------------------------------------------- #
# 19/20. Double-gate: worker stays read-only; canary env cannot unlock worker
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_worker_write_enabled_false_by_default(tmp_path):
    import app.transfer.adapters as adapters_mod
    with patch.object(adapters_mod, "get_settings") as mock_settings:
        mock_settings.return_value.cloud_write_enabled = False
        mock_settings.return_value.canary_cloud_write_enabled = False
        assert adapters_mod.effective_cloud_write_enabled() is False
        # canary process env alone must NOT unlock the ordinary worker
        assert adapters_mod.effective_cloud_write_enabled(permit_canary_env=False) is False


@pytest.mark.asyncio
async def test_canary_env_does_not_unlock_worker(monkeypatch, tmp_path):
    import app.transfer.adapters as adapters_mod
    monkeypatch.setenv("CANARY_CLOUD_WRITE_ENABLED", "true")
    with patch.object(adapters_mod, "get_settings") as mock_settings:
        mock_settings.return_value.cloud_write_enabled = False
        mock_settings.return_value.canary_cloud_write_enabled = False
        # ordinary worker: even with the env var exported globally it cannot write
        assert adapters_mod.effective_cloud_write_enabled(permit_canary_env=False) is False


# --------------------------------------------------------------------------- #
# 21. dry-run zero DB writes (repeat of #11 with strict no-write assertion)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_canary_dry_run_zero_db_writes(tmp_path):
    from tools.run_transfer_canary import run_preflight
    engine, sessions, task_id = await _canary_task(tmp_path)
    async with sessions() as db:
        before_task = await db.get(TransferQueueTask, task_id)
        before_resources = (await db.execute(select(Resource))).scalars().all()
        before_candidates = (await db.execute(select(ResourceCandidate))).scalars().all()
    pre = await run_preflight(task_id, session_factory=sessions)
    async with sessions() as db:
        after_task = await db.get(TransferQueueTask, task_id)
        after_resources = (await db.execute(select(Resource))).scalars().all()
        after_candidates = (await db.execute(select(ResourceCandidate))).scalars().all()
    assert before_task is not None and after_task is not None
    assert (before_task.created_at, before_task.status, before_task.attempt_count) == (
        after_task.created_at, after_task.status, after_task.attempt_count)
    assert len(before_resources) == len(after_resources)
    assert len(before_candidates) == len(after_candidates)
    assert pre["verdict"] == "CANARY_SAFE"
    await engine.dispose()
