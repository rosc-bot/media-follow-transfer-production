from collections.abc import Collection

from app.follow.episode_keys import canonical_episode_key
from app.follow.follow_mode import FULL, normalize_follow_mode


class MissingEpisodeService:
    @staticmethod
    def _canonical_collected_keys(watchlist) -> set[str]:
        """Normalize all legacy collected representations to one key format."""
        season = int(watchlist.season or 1)
        return {
            key
            for value in (watchlist.collected_episodes or [])
            if (key := canonical_episode_key(season, value)) is not None
        }

    @staticmethod
    def missing_for_watchlist(
        watchlist,
        *,
        follow_mode: object | None = None,
        recent_limit: int | None = 30,
        ignored_episodes: Collection[int] = (),
    ) -> list[str]:
        """Return canonical aired-but-uncollected, non-ignored episode keys.

        ``FULL`` walks E01 through ``last_aired_episode`` with no recent
        truncation. ``LATEST`` retains the configured recent window. Legacy
        ``ALL`` and ``AUTO`` are interpreted by ``normalize_follow_mode``;
        persisted rows are intentionally not rewritten just by reading them.
        """
        total = watchlist.last_aired_episode or watchlist.total_episodes or 0
        if total <= 0:
            return []
        ignored = {int(episode) for episode in ignored_episodes}
        if 0 in ignored:
            return []
        mode = normalize_follow_mode(follow_mode if follow_mode is not None else watchlist.follow_mode)
        start_ep = 1 if mode == FULL else max(1, total - int(recent_limit or 0) + 1)
        season = int(watchlist.season or 1)
        collected = MissingEpisodeService._canonical_collected_keys(watchlist)
        return [
            f'S{season:02d}E{episode:02d}'
            for episode in range(start_ep, total + 1)
            if episode not in ignored and f'S{season:02d}E{episode:02d}' not in collected
        ]
