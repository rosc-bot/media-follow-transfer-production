import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy import select
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
from app.models.cloud import CloudConfig
from app.models.watchlist import SeriesWatchlist
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


async def _collect_promotion_physical_evidence(
    db: AsyncSession,
    candidates: list[SeriesWatchlist],
) -> tuple[dict[int, dict[str, Any]], dict[int, bool]]:
    """Read candidate series roots before a Promotion task can be enqueued.

    This uses only the Guangya list APIs.  Any identity, provider, auth or
    physical-scan problem is represented as unverified evidence and therefore
    fails closed in ``enqueue_ready_promotions``.
    """
    scans: dict[int, dict[str, Any]] = {}
    conflicts: dict[int, bool] = {}
    for watchlist in candidates:
        tmdb_id = int(watchlist.tmdb_id)
        try:
            resource = await CompletionPromotionService._latest_resource(
                db,
                tmdb_id,
                int(watchlist.season or 1),
            )
            provider = str(resource.cloud_name if resource else '').casefold()
            config = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
            seasons = [
                int(row.season or 1)
                for row in (await db.scalars(select(SeriesWatchlist).where(
                    SeriesWatchlist.tmdb_id == tmdb_id,
                    SeriesWatchlist.status != 'CANCELLED',
                ).order_by(SeriesWatchlist.season.asc()))).all()
            ]
            if (
                resource is None
                or provider != 'guangya'
                or config is None
                or not config.enabled
                or not config.auth_ref
                or not config.target_folder_id
                or not watchlist.remote_series_folder_id
                or not seasons
            ):
                raise RuntimeError('PROMOTION_PHYSICAL_SCAN_IDENTITY_OR_PROVIDER_UNAVAILABLE')
            adapter = GuangyaAdapter(write_enabled=False)
            scan = await adapter.scan_series_root_readonly(
                auth_token=str(config.auth_ref),
                tmdb_id=tmdb_id,
                series_root_id=str(watchlist.remote_series_folder_id),
                relevant_seasons=seasons,
                timeout_seconds=30,
                max_depth=6,
                max_items=5000,
                page_size=100,
            )
            conflict = await adapter.inspect_completed_root_conflict_readonly(
                auth_token=str(config.auth_ref),
                completed_root_id=str(config.target_folder_id),
                tmdb_id=tmdb_id,
                title=str(watchlist.title or resource.title or ''),
            )
            scans[tmdb_id] = dict(scan)
            conflicts[tmdb_id] = bool(conflict.get('conflict'))
        except Exception as exc:  # noqa: BLE001 - promotion must remain fail-closed
            logger.warning('Promotion physical scan unavailable for tmdb=%s: %s', tmdb_id, exc)
            scans[tmdb_id] = {
                'scan_status': 'API_ERROR',
                'scan_timestamp': None,
                'scan_watermark': None,
                'cloud_episode_keys_by_season': {},
                'error': f'{type(exc).__name__}: {str(exc)[:240]}',
            }
            conflicts[tmdb_id] = True
    return scans, conflicts


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
    promotion_candidates = await CompletionPromotionService.prefilter_watchlists(db)
    physical_scan_results, destination_conflicts = await _collect_promotion_physical_evidence(
        db,
        promotion_candidates,
    )
    promotions = await CompletionPromotionService.enqueue_ready_promotions(
        db,
        physical_scan_results=physical_scan_results,
        destination_conflicts=destination_conflicts,
    )
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
        'transfer_eligible=%d new_queue_tasks_created=%d deduplicated_existing_tasks=%d queue_reused=%d needs_review=%d '
        'missing_total=%d missing_with_local_candidate=%d missing_with_framehdr_candidate=%d auto_safe=%d '
        'pending_review=%d no_resource=%d queued_new=%d queued_reactivated=%d candidate_switched=%d stale_pending_recovered=%d)',
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
        cycle_stats.get('missing_total', 0),
        cycle_stats.get('missing_with_local_candidate', 0),
        cycle_stats.get('missing_with_framehdr_candidate', 0),
        cycle_stats.get('auto_safe', 0),
        cycle_stats.get('pending_review', 0),
        cycle_stats.get('no_resource', 0),
        cycle_stats.get('queued_new', 0),
        cycle_stats.get('queued_reactivated', 0),
        cycle_stats.get('candidate_switched', 0),
        cycle_stats.get('stale_pending_recovered', 0),
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
