"""Fail-closed cloud-root routing for completed versus ongoing media."""

from dataclasses import dataclass
from typing import Any

from app.follow.episode_keys import canonical_episode_key

_COMPLETED_SERIES_STATUSES = frozenset({'ended', 'canceled', 'cancelled'})
_TV_TYPES = frozenset({'tv', 'anime', 'series', '电视剧', '动漫'})


@dataclass(frozen=True)
class DestinationRoute:
    kind: str
    target_folder_id: str


class DestinationRouter:
    """Select only a configured root; never fall back to the drive root."""

    @staticmethod
    def _canonical_episode_keys(values: list[Any] | None, *, season: int | None) -> set[str]:
        return {
            key
            for value in values or []
            if (key := canonical_episode_key(season, value)) is not None
        }

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
        operation: str = 'transfer',
        physical_complete: bool = False,
    ) -> DestinationRoute:
        completed_root = str(getattr(cloud_config, 'target_folder_id', '') or '').strip()
        if not completed_root:
            raise ValueError('target_folder_id (影视转存总目录) is required')
        media_type = str(getattr(resource, 'media_type', 'tv') or 'tv').casefold()
        is_tv = media_type in _TV_TYPES
        is_complete = not is_tv or (
            str(operation or 'transfer').casefold() == 'promote'
            and physical_complete
            and cls._is_verified_complete(
                resource=resource,
                watchlist=watchlist,
                incoming_episode_keys=incoming_episode_keys,
            )
        )
        if is_complete:
            return DestinationRoute(kind='completed', target_folder_id=completed_root)
        ongoing_root = str(getattr(cloud_config, 'ongoing_target_folder_id', '') or '').strip()
        if not ongoing_root:
            raise ValueError('ongoing_target_folder_id (追新未完结目录) is required for unfinished series')
        return DestinationRoute(kind='ongoing', target_folder_id=ongoing_root)
