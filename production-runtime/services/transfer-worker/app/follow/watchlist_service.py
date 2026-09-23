from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.follow.episode_keys import canonical_episode_key, episode_sort_key
from app.follow.follow_mode import normalize_follow_mode
from app.models.watchlist import SeriesWatchlist


class WatchlistService:
    @staticmethod
    async def add(db: AsyncSession, **data) -> SeriesWatchlist:
        season = int(data.get('season', 1) or 1)
        data['season'] = season
        data['follow_mode'] = normalize_follow_mode(data.get('follow_mode'))
        raw_collected = list(data.get('collected_episodes') or [])
        normalized = {
            key
            for value in raw_collected
            if (key := canonical_episode_key(season, value)) is not None
        }
        data['collected_episodes'] = sorted(normalized, key=episode_sort_key)
        existing = await db.scalar(select(SeriesWatchlist).where(SeriesWatchlist.tmdb_id == data['tmdb_id'], SeriesWatchlist.season == season, SeriesWatchlist.subscriber_tg_id == data.get('subscriber_tg_id')))
        if existing: return existing
        row=SeriesWatchlist(**data); db.add(row); await db.flush(); return row

    @staticmethod
    async def list_following(db: AsyncSession) -> list[SeriesWatchlist]:
        return list((await db.scalars(select(SeriesWatchlist).where(SeriesWatchlist.status == 'FOLLOWING').order_by(SeriesWatchlist.updated_at))).all())

    @staticmethod
    async def mark_collected(db: AsyncSession, *, tmdb_id: int, season: int, episode_keys: list[str]) -> SeriesWatchlist | None:
        row=await db.scalar(select(SeriesWatchlist).where(SeriesWatchlist.tmdb_id == tmdb_id, SeriesWatchlist.season == season))
        if not row: return None
        raw_values = list(row.collected_episodes or [])
        canonical_values = {
            key
            for value in raw_values + list(episode_keys or [])
            if (key := canonical_episode_key(season, value)) is not None
        }
        unknown_values = [
            str(value).strip()
            for value in raw_values
            if canonical_episode_key(season, value) is None and str(value).strip()
        ]
        row.collected_episodes = unknown_values + sorted(canonical_values, key=episode_sort_key)
        await db.flush(); return row
