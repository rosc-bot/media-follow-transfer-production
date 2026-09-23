import logging
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.follow.calendar_service import CalendarService
from app.follow.watchlist_service import WatchlistService

logger = logging.getLogger(__name__)


class ScheduleService:
    def __init__(self, calendar: CalendarService, *, lifecycle_refresh_limit: int = 20):
        self.calendar = calendar
        self.lifecycle_refresh_limit = max(0, lifecycle_refresh_limit)

    async def sync_watchlist(self, db: AsyncSession) -> int:
        """Sync each show independently and refresh a bounded lifecycle subset."""
        rows = await WatchlistService.list_following(db)
        changed = 0
        lifecycle_refreshes = 0
        status_fetcher: Callable[[int], Awaitable[Any]] | None = getattr(
            self.calendar.provider, 'fetch_series_status', None,
        )
        for row in rows:
            if row.tmdb_id <= 0 or row.season <= 0:
                continue
            try:
                data = await self.calendar.sync(row)
            except (RuntimeError, ValueError, TypeError) as exc:
                # Keep previously verified schedule values. The next cycle may
                # recover after TMDB/provider data is repaired.
                logger.warning(
                    'Skipping schedule sync for %s S%02d (TMDB %s): %s',
                    row.title, row.season, row.tmdb_id, exc,
                )
                continue
            if (
                not row.tmdb_series_status
                and status_fetcher is not None
                and lifecycle_refreshes < self.lifecycle_refresh_limit
            ):
                lifecycle_refreshes += 1
                try:
                    lifecycle_status = await status_fetcher(row.tmdb_id)
                except (RuntimeError, ValueError, TypeError) as exc:
                    logger.warning('Skipping lifecycle refresh for %s (TMDB %s): %s', row.title, row.tmdb_id, exc)
                else:
                    if lifecycle_status:
                        data['tmdb_series_status'] = lifecycle_status
            for key in ('total_episodes', 'last_aired_episode', 'tmdb_series_status'):
                if data.get(key) is not None:
                    setattr(row, key, data[key])
            changed += 1
        await db.flush()
        return changed
