from sqlalchemy.ext.asyncio import AsyncSession

from app.follow.ignored_missing_service import IgnoredMissingService
from app.follow.missing_episode_service import MissingEpisodeService


class RadarService:
    @staticmethod
    async def build(
        db: AsyncSession,
        watchlists,
        *,
        follow_mode: object | None = None,
        recent_limit: int | None = 30,
    ) -> list[dict]:
        """Use the exact same missing semantics as the follow worker.

        ``follow_mode`` selects the view scope without changing persisted
        per-watchlist policy. The worker passes no override and uses each row.
        """
        ignored_index = await IgnoredMissingService.load_index(db)
        return [
            {
                "watchlist_id": row.id,
                "tmdb_id": row.tmdb_id,
                "title": row.title,
                "season": row.season,
                "missing_episodes": MissingEpisodeService.missing_for_watchlist(
                    row,
                    follow_mode=follow_mode,
                    recent_limit=recent_limit,
                    ignored_episodes=IgnoredMissingService.episodes_for(row, ignored_index),
                ),
            }
            for row in watchlists
        ]
