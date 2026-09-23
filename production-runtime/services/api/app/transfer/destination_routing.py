"""Fail-closed lifecycle routing backed by one canonical TMDB destination."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from app.follow.episode_keys import canonical_episode_key
from app.transfer.canonical_destination import (
    CanonicalDestination,
    CanonicalDestinationBuilder,
    DestinationMetadataIncomplete,
)

_COMPLETED_SERIES_STATUSES = frozenset({'ended', 'canceled', 'cancelled'})
_TV_TYPES = frozenset({'tv', 'anime', 'series', '电视剧', '动漫'})


@dataclass(frozen=True)
class DestinationRoute:
    kind: str
    target_folder_id: str
    destination: CanonicalDestination | None = None

    @property
    def media_root(self) -> str | None:
        return self.destination.media_root if self.destination else None

    @property
    def media_category(self) -> str | None:
        return self.destination.media_category if self.destination else None

    @property
    def series_folder_name(self) -> str | None:
        return self.destination.item_name if self.destination else None

    @property
    def season_folder_name(self) -> str | None:
        return self.destination.season_name if self.destination else None

    @property
    def inventory_prefix(self) -> str | None:
        return self.destination.inventory_prefix if self.destination else None

    @property
    def archive_directory(self) -> str | None:
        return self.destination.archive_directory if self.destination else None


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
        metadata: Mapping[str, Any] | None = None,
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
        kind = 'completed' if is_complete else 'ongoing'
        target_root = completed_root
        if not is_complete:
            target_root = str(getattr(cloud_config, 'ongoing_target_folder_id', '') or '').strip()
            if not target_root:
                raise ValueError('ongoing_target_folder_id (追新未完结目录) is required for unfinished series')
        if metadata is None:
            raise DestinationMetadataIncomplete(
                'TMDB metadata is required before destination routing; refusing an unclassified restore'
            )
        title = str(getattr(resource, 'title', '') or getattr(watchlist, 'title', '') or '').strip()
        year = getattr(resource, 'year', None)
        if year is None and watchlist is not None:
            year = getattr(watchlist, 'year', None)
        try:
            destination = CanonicalDestinationBuilder.build(
                metadata=metadata,
                tmdb_id=int(getattr(resource, 'tmdb_id', 0) or metadata.get('id') or metadata.get('tmdb_id') or 0),
                media_type=media_type,
                title=title,
                year=int(year) if year is not None else None,
                destination_kind=kind,
                season=(getattr(resource, 'season', None) or getattr(watchlist, 'season', None)),
            )
        except (TypeError, ValueError, DestinationMetadataIncomplete) as exc:
            raise DestinationMetadataIncomplete(str(exc)) from exc
        return DestinationRoute(kind=kind, target_folder_id=target_root, destination=destination)
