"""Fail-closed cloud-root routing for completed versus ongoing media."""

import re
from dataclasses import dataclass
from typing import Any

_COMPLETED_SERIES_STATUSES = frozenset({'ended', 'canceled', 'cancelled'})
_TV_TYPES = frozenset({'tv', 'anime', 'series', '电视剧', '动漫'})
_EPISODE_RE = re.compile(r'(?:(?:S\d{1,3})?E)?(\d{1,4})$', re.IGNORECASE)


@dataclass(frozen=True)
class DestinationRoute:
    kind: str
    target_folder_id: str


class DestinationRouter:
    """Select only a configured root; never fall back to the drive root."""

    @staticmethod
    def _canonical_episode_keys(values: list[Any] | None, *, season: int | None) -> set[str]:
        resolved_season = int(season) if season is not None else 1
        keys: set[str] = set()
        for value in values or []:
            if isinstance(value, bool):
                continue
            text = str(value).strip()
            match = _EPISODE_RE.search(text)
            if not match:
                continue
            episode = int(match.group(1))
            if episode > 0:
                keys.add(f'S{resolved_season:02d}E{episode:02d}')
        return keys

    @classmethod
    def _is_verified_complete(cls, *, resource: Any, watchlist: Any | None, incoming_episode_keys: list[str]) -> bool:
        if watchlist is None:
            return False
        status = str(getattr(watchlist, 'tmdb_series_status', '') or '').casefold()
        total = getattr(watchlist, 'total_episodes', None)
        try:
            total = int(total)
        except (TypeError, ValueError):
            return False
        if status not in _COMPLETED_SERIES_STATUSES or total <= 0:
            return False
        season = getattr(resource, 'season', None) or getattr(watchlist, 'season', None)
        collected = cls._canonical_episode_keys(getattr(watchlist, 'collected_episodes', None), season=season)
        incoming = cls._canonical_episode_keys(incoming_episode_keys, season=season)
        return len(collected | incoming) >= total

    @classmethod
    def resolve(
        cls,
        *,
        resource: Any,
        cloud_config: Any,
        watchlist: Any | None,
        incoming_episode_keys: list[str],
    ) -> DestinationRoute:
        completed_root = str(getattr(cloud_config, 'target_folder_id', '') or '').strip()
        if not completed_root:
            raise ValueError('target_folder_id (影视转存总目录) is required')
        media_type = str(getattr(resource, 'media_type', 'tv') or 'tv').casefold()
        is_tv = media_type in _TV_TYPES
        is_complete = not is_tv or cls._is_verified_complete(
            resource=resource,
            watchlist=watchlist,
            incoming_episode_keys=incoming_episode_keys,
        )
        if is_complete:
            return DestinationRoute(kind='completed', target_folder_id=completed_root)
        ongoing_root = str(getattr(cloud_config, 'ongoing_target_folder_id', '') or '').strip()
        if not ongoing_root:
            raise ValueError('ongoing_target_folder_id (追新未完结目录) is required for unfinished series')
        return DestinationRoute(kind='ongoing', target_folder_id=ongoing_root)
