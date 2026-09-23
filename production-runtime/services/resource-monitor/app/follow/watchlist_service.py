from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.follow.follow_mode import normalize_follow_mode
from app.models.watchlist import SeriesWatchlist


class WatchlistService:
    @staticmethod
    async def add(db: AsyncSession, **data) -> SeriesWatchlist:
        data['follow_mode'] = normalize_follow_mode(data.get('follow_mode'))
        existing = await db.scalar(select(SeriesWatchlist).where(SeriesWatchlist.tmdb_id == data['tmdb_id'], SeriesWatchlist.season == data.get('season', 1), SeriesWatchlist.subscriber_tg_id == data.get('subscriber_tg_id')))
        if existing: return existing
        row=SeriesWatchlist(**data); db.add(row); await db.flush(); return row

    @staticmethod
    async def list_following(db: AsyncSession) -> list[SeriesWatchlist]:
        return list((await db.scalars(select(SeriesWatchlist).where(SeriesWatchlist.status == 'FOLLOWING').order_by(SeriesWatchlist.updated_at))).all())

    @staticmethod
    async def mark_collected(db: AsyncSession, *, tmdb_id: int, season: int, episode_keys: list[str]) -> SeriesWatchlist | None:
        row=await db.scalar(select(SeriesWatchlist).where(SeriesWatchlist.tmdb_id == tmdb_id, SeriesWatchlist.season == season))
        if not row: return None
        row.collected_episodes=sorted(set(row.collected_episodes or []) | set(episode_keys)); await db.flush(); return row
