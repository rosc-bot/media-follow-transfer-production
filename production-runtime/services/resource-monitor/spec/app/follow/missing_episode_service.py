import re


class MissingEpisodeService:
    @staticmethod
    def _canonical_collected_keys(watchlist) -> set[str]:
        """Normalize legacy integer/E## values and canonical S##E## values.

        Legacy watchlist imports stored some collected episodes as integers.  A
        direct string comparison makes every such file look missing, so all
        accepted representations are converted to the current season key.
        """
        season = int(watchlist.season or 1)
        keys: set[str] = set()
        for value in watchlist.collected_episodes or []:
            if isinstance(value, int):
                keys.add(f'S{season:02d}E{value:02d}')
                continue
            text = str(value).strip().upper()
            matched = re.fullmatch(r'S(\d{1,2})E(\d{1,4})', text)
            if matched:
                keys.add(f'S{int(matched.group(1)):02d}E{int(matched.group(2)):02d}')
                continue
            matched = re.fullmatch(r'E?(\d{1,4})', text)
            if matched:
                keys.add(f'S{season:02d}E{int(matched.group(1)):02d}')
        return keys

    @staticmethod
    def missing_for_watchlist(watchlist, *, recent_limit: int | None = 30) -> list[str]:
        """Return episode keys not yet collected, scoped to the recent window.

        Total is derived from last_aired_episode (or total_episodes).  When
        ``recent_limit`` is set, only the last N episodes are considered
        missing -- legacy gaps deep in the season are skipped instead of being
        re-scouted every cycle (each missing episode costs multiple FrameHdr
        queries, so unbounded missing lists stall the whole follow loop).
        """
        total = watchlist.last_aired_episode or watchlist.total_episodes or 0
        if total <= 0:
            return []
        start_ep = max(1, total - int(recent_limit or 0) + 1) if recent_limit is not None else 1
        collected = MissingEpisodeService._canonical_collected_keys(watchlist)
        return [
            f'S{watchlist.season:02d}E{ep:02d}'
            for ep in range(start_ep, total + 1)
            if f'S{watchlist.season:02d}E{ep:02d}' not in collected
        ]
