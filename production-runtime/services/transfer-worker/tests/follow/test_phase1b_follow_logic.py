from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.follow.follow_worker import run_follow_cycle
from app.follow.missing_episode_service import MissingEpisodeService
from app.follow.radar_service import RadarService
from app.models.bot_settings import BotSettings
from app.models.ignored_missing import IgnoredMissing
from app.models.watchlist import SeriesWatchlist


def make_watchlist(*, mode="LATEST", collected=None, aired=40):
    return SimpleNamespace(
        id=1,
        tmdb_id=100,
        title="模式测试剧",
        season=1,
        follow_mode=mode,
        last_aired_episode=aired,
        total_episodes=aired,
        collected_episodes=list(collected or []),
    )


def test_full_uses_full_aired_horizon_and_legacy_modes_are_compatible():
    full = MissingEpisodeService.missing_for_watchlist(make_watchlist(mode="FULL"), recent_limit=30)
    legacy_all = MissingEpisodeService.missing_for_watchlist(make_watchlist(mode="ALL"), recent_limit=30)
    latest = MissingEpisodeService.missing_for_watchlist(make_watchlist(mode="LATEST"), recent_limit=30)
    legacy_auto = MissingEpisodeService.missing_for_watchlist(make_watchlist(mode="AUTO"), recent_limit=30)

    assert full == [f"S01E{episode:02d}" for episode in range(1, 41)]
    assert legacy_all == full
    assert latest == [f"S01E{episode:02d}" for episode in range(11, 41)]
    assert legacy_auto == latest


def test_collected_and_ignored_are_excluded_from_the_same_algorithm():
    row = make_watchlist(mode="FULL", collected=[1, "E02", "S01E03"], aired=5)
    assert MissingEpisodeService.missing_for_watchlist(
        row,
        recent_limit=30,
        ignored_episodes={4},
    ) == ["S01E05"]
    assert MissingEpisodeService.missing_for_watchlist(
        row,
        recent_limit=30,
        ignored_episodes={0},
    ) == []


class NoopCalendar:
    def __init__(self):
        self.provider = self

    async def fetch_schedule(self, tmdb_id, season):
        return {"total_episodes": 40, "last_aired_episode": 40}

    async def fetch_series_status(self, _tmdb_id):
        return None

    async def sync(self, row):
        data = await self.fetch_schedule(row.tmdb_id, row.season)
        row.total_episodes = data["total_episodes"]
        row.last_aired_episode = data["last_aired_episode"]
        return data


class RecordingScout:
    def __init__(self):
        self.calls = []

    async def scout_missing(self, _db, **kwargs):
        self.calls.append(kwargs)
        return []


@pytest.mark.asyncio
async def test_worker_respects_each_row_mode_and_ignore_rules(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add_all([
            BotSettings(key="global_pause", val="0"),
            BotSettings(key="follow_paused", val="0"),
            SeriesWatchlist(tmdb_id=101, title="全量剧", season=1, status="FOLLOWING", follow_mode="FULL"),
            SeriesWatchlist(tmdb_id=102, title="最新剧", season=1, status="FOLLOWING", follow_mode="LATEST"),
            SeriesWatchlist(tmdb_id=103, title="忽略剧", season=1, status="FOLLOWING", follow_mode="FULL"),
            IgnoredMissing(title="忽略剧", season=1, episode=0),
        ])
    scout = RecordingScout()
    async with sessions() as db, db.begin():
        await run_follow_cycle(db, calendar=NoopCalendar(), scout=scout, recent_episode_window=30)
    by_id = {call["tmdb_id"]: call["missing_episodes"] for call in scout.calls}
    assert by_id[101] == [f"S01E{episode:02d}" for episode in range(1, 41)]
    assert by_id[102] == [f"S01E{episode:02d}" for episode in range(11, 41)]
    assert 103 not in by_id
    await engine.dispose()


@pytest.mark.asyncio
async def test_radar_uses_same_mode_and_ignore_algorithm_as_worker(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/app.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        row = SeriesWatchlist(
            tmdb_id=201,
            title="雷达剧",
            season=1,
            status="FOLLOWING",
            follow_mode="FULL",
            last_aired_episode=40,
            total_episodes=40,
            collected_episodes=["S01E01"],
        )
        db.add_all([row, IgnoredMissing(title="雷达剧", season=1, episode=40)])
    async with sessions() as db:
        rows = [row]
        latest = await RadarService.build(db, rows, follow_mode="LATEST", recent_limit=30)
        full = await RadarService.build(db, rows, follow_mode="FULL", recent_limit=30)
    assert latest[0]["missing_episodes"] == [f"S01E{episode:02d}" for episode in range(11, 40)]
    assert full[0]["missing_episodes"] == [f"S01E{episode:02d}" for episode in range(2, 40)]
    await engine.dispose()
