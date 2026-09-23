import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.database import AsyncSessionLocal
from app.core.logging import configure_logging
from app.follow.bot_settings_service import BotSettingsService
from app.follow.calendar_service import CalendarService
from app.follow.completion_promotion_service import CompletionPromotionService
from app.follow.follow_mode import normalize_follow_mode
from app.follow.ignored_missing_service import IgnoredMissingService
from app.follow.legacy_root_discovery_service import LegacyRootDiscoveryService
from app.follow.missing_episode_service import MissingEpisodeService
from app.follow.schedule_service import ScheduleService
from app.follow.tmdb_provider import TMDBSeasonProvider
from app.follow.watchlist_service import WatchlistService
from app.scout.message_search import MessageSearch
from app.scout.scout_service import ScoutService
from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.guangya_auth import GuangyaCredentialStore

logger = logging.getLogger(__name__)


async def _list_legacy_directories(*, provider: str, auth_token: str, parent_id: str) -> list[dict]:
    if provider != 'guangya':
        raise ValueError(f'legacy root discovery is unsupported for provider {provider}')
    store = GuangyaCredentialStore(AsyncSessionLocal)
    return await GuangyaAdapter(write_enabled=False, credential_store=store).list_directories(
        auth_token=auth_token,
        parent_id=parent_id,
    )


async def run_follow_cycle(
    db: AsyncSession,
    *,
    calendar: CalendarService,
    scout: ScoutService,
    recent_episode_window: int = 30,
) -> dict[str, Any]:
    """Sync TMDB metadata first, then scout only the still-missing episodes."""
    if await BotSettingsService.is_follow_paused(db):
        logger.info('Follow cycle skipped: follow pause is enabled')
        return {'synced_watchlists': 0, 'scout_jobs': 0}

    synced = await ScheduleService(calendar).sync_watchlist(db)
    discovered = await LegacyRootDiscoveryService(_list_legacy_directories).backfill(db)
    promotions = await CompletionPromotionService.enqueue_ready_promotions(db)
    rows = await WatchlistService.list_following(db)
    ignored_index = await IgnoredMissingService.load_index(db)
    scout_jobs = 0
    cycle_stats = ScoutService.summarize([])
    for row in rows:
        mode = normalize_follow_mode(row.follow_mode)
        missing = MissingEpisodeService.missing_for_watchlist(
            row,
            follow_mode=mode,
            recent_limit=recent_episode_window,
            ignored_episodes=IgnoredMissingService.episodes_for(row, ignored_index),
        )
        if not missing:
            continue
        results = await scout.scout_missing(
            db,
            tmdb_id=row.tmdb_id,
            title=row.title,
            season=row.season,
            missing_episodes=missing,
            year=row.year,
        )
        scout_jobs += len(results)
        for key, value in ScoutService.summarize(results).items():
            cycle_stats[key] = cycle_stats.get(key, 0) + value
    logger.info(
        'Follow cycle complete: synced %d watchlists, discovered %d legacy roots, queued %d promotions, scouted %d jobs '
        '(targets=%d local_hit=%d local_miss=%d framehdr_attempted=%d framehdr_hit=%d framehdr_miss=%d '
        'transfer_eligible=%d new_queue_tasks_created=%d deduplicated_existing_tasks=%d queue_reused=%d needs_review=%d)',
        synced,
        discovered,
        promotions,
        scout_jobs,
        cycle_stats.get('target_episodes', 0),
        cycle_stats.get('local_hit', 0),
        cycle_stats.get('local_miss', 0),
        cycle_stats.get('framehdr_attempted', 0),
        cycle_stats.get('framehdr_hit', 0),
        cycle_stats.get('framehdr_miss', 0),
        cycle_stats.get('transfer_eligible', 0),
        cycle_stats.get('new_queue_tasks_created', 0),
        cycle_stats.get('deduplicated_existing_tasks', 0),
        cycle_stats.get('queue_reused', 0),
        cycle_stats.get('needs_review', 0),
    )
    return {'synced_watchlists': synced, 'scout_jobs': scout_jobs, 'cycle_stats': cycle_stats}


async def run_once() -> dict:
    settings = get_settings()
    calendar = CalendarService(TMDBSeasonProvider(api_key=settings.tmdb_api_key, base_url=settings.tmdb_base_url))
    scout = ScoutService(MessageSearch(settings.resource_messages_db))
    async with AsyncSessionLocal() as db, db.begin():
        return await run_follow_cycle(
            db,
            calendar=calendar,
            scout=scout,
            recent_episode_window=settings.follow_recent_episode_window,
        )


async def run_follow_worker(
    *,
    interval_seconds: float = 900,
    sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
) -> None:
    configure_logging()
    logger.info("Follow worker started (interval=%ss)...", interval_seconds)
    while True:
        try:
            await run_once()
        except Exception:
            logger.exception("Unexpected error in follow worker cycle")
        await sleep(interval_seconds)


if __name__ == '__main__':
    asyncio.run(run_follow_worker(interval_seconds=get_settings().follow_poll_interval_seconds))
