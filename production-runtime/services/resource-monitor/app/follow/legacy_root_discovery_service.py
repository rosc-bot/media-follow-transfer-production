"""Read-only discovery of pre-migration series roots in the ongoing cloud root."""

import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.cloud import CloudConfig
from app.models.resource import Resource
from app.models.watchlist import SeriesWatchlist
from app.transfer.errors import GuangyaTransferError

DirectoryLister = Callable[..., Awaitable[list[dict[str, Any]]]]
logger = logging.getLogger(__name__)


class LegacyRootDiscoveryService:
    """Backfill only one exact `{tmdbid-N}` root; ambiguity is deliberately ignored."""

    def __init__(self, list_directories: DirectoryLister) -> None:
        self.list_directories = list_directories

    @staticmethod
    def _matching_folder_id(folders: list[dict[str, Any]], tmdb_id: int) -> str | None:
        marker = re.compile(rf'(?<!\d)tmdbid-{re.escape(str(tmdb_id))}(?!\d)', re.IGNORECASE)
        candidates = [
            str(item.get('fileId') or item.get('id') or '').strip()
            for item in folders
            if item.get('resType') == 2 and marker.search(str(item.get('name') or item.get('fileName') or ''))
        ]
        candidates = [candidate for candidate in candidates if candidate]
        return candidates[0] if len(candidates) == 1 else None

    async def backfill(self, db: AsyncSession) -> int:
        rows = list((await db.scalars(select(SeriesWatchlist).where(
            SeriesWatchlist.remote_series_folder_id.is_(None),
        ))).all())
        by_provider: dict[str, list[SeriesWatchlist]] = {}
        for watchlist in rows:
            resource = await db.scalar(select(Resource).where(
                Resource.tmdb_id == watchlist.tmdb_id,
                Resource.season == watchlist.season,
                Resource.cloud_name.is_not(None),
            ).order_by(Resource.id.desc()))
            if resource is None:
                continue
            provider = str(resource.cloud_name or '').strip().lower()
            if provider:
                by_provider.setdefault(provider, []).append(watchlist)

        discovered = 0
        for provider, watchlists in by_provider.items():
            config = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
            root_id = str(getattr(config, 'ongoing_target_folder_id', '') or '').strip() if config else ''
            auth_token = str(getattr(config, 'auth_ref', '') or '').strip() if config else ''
            if not config or not config.enabled or not root_id or not auth_token:
                continue
            try:
                folders = await self.list_directories(
                    provider=provider,
                    auth_token=auth_token,
                    parent_id=root_id,
                )
            except GuangyaTransferError as auth_exc:
                # Credential/network problems must never break the Follow cycle:
                # legacy discovery degrades to a structured, stack-trace-free skip.
                logger.warning(
                    'Legacy root discovery skipped for provider %s: Guangya authentication unavailable (%s)',
                    provider,
                    auth_exc.category,
                )
                continue
            except (httpx.HTTPError, RuntimeError, ValueError, OSError) as exc:
                logger.warning('Legacy root discovery skipped for provider %s: %s', provider, exc)
                continue
            for watchlist in watchlists:
                folder_id = self._matching_folder_id(folders, watchlist.tmdb_id)
                if not folder_id:
                    continue
                watchlist.remote_series_folder_id = folder_id
                watchlist.remote_destination_kind = 'ongoing'
                discovered += 1
        await db.flush()
        return discovered
