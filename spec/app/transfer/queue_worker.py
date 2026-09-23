import logging
import os
import socket

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.follow.bot_settings_service import BotSettingsService
from app.follow.watchlist_service import WatchlistService
from app.models.cloud import CloudConfig
from app.models.resource import Resource
from app.models.watchlist import SeriesWatchlist
from app.transfer.destination_routing import DestinationRouter
from app.transfer.notifier import TransferNotifier
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.queue_service import TransferQueueService

logger = logging.getLogger(__name__)

_SERIES_MEDIA_TYPES = frozenset({'tv', 'anime', 'series', '电视剧', '动漫'})


def _series_layout_names(resource: Resource, watchlist: SeriesWatchlist | None, destination_kind: str) -> tuple[str, str] | None:
    """Return a stable series root and season child only for episodic media."""
    if str(resource.media_type or 'tv').casefold() not in _SERIES_MEDIA_TYPES:
        return None
    title = str(resource.title or getattr(watchlist, 'title', '') or '').strip()
    if not title:
        raise RuntimeError('series transfer requires a verified title before creating a remote directory')
    year = resource.year if resource.year is not None else getattr(watchlist, 'year', None)
    pieces = [title]
    if year:
        pieces.append(f'({int(year)})')
    if resource.tmdb_id:
        pieces.append(f'{{tmdbid-{int(resource.tmdb_id)}}}')
    series_name = ' '.join(pieces)
    if destination_kind == 'completed':
        series_name = f'{series_name}【完结】'
    season = int(resource.season or getattr(watchlist, 'season', 1) or 1)
    if season < 1:
        raise RuntimeError('series transfer requires a positive season number')
    return series_name, f'S{season:02d}'


class TransferQueueWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        orchestrator: TransferOrchestrator | None = None,
        worker_id: str | None = None,
        notifier: TransferNotifier | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.orchestrator = orchestrator or TransferOrchestrator()
        self.worker_id = worker_id or f'{socket.gethostname()}:{os.getpid()}'
        self.notifier = notifier if notifier is not None else TransferNotifier()

    async def process_once(self) -> bool:
        async with self.session_factory() as db, db.begin():
            if await BotSettingsService.is_global_paused(db):
                logger.info('Transfer queue consumption skipped: global pause is enabled')
                return False
            task = await TransferQueueService.claim_next(db, worker_id=self.worker_id)
            if not task:
                return False
            payload = dict(task.payload or {})
            resource = await db.get(Resource, task.resource_id)
            try:
                is_promotion = str(payload.get('operation') or '').strip().lower() == 'promote'
                provider = str(payload.get('provider') or (resource.cloud_name if resource else '') or 'guangya').lower()
                payload['provider'] = provider
                watchlist = None

                if resource:
                    if not payload.get('share_url') and resource.share_url:
                        payload['share_url'] = resource.share_url
                    if not payload.get('expected_files') and resource.file_names:
                        payload['expected_files'] = list(resource.file_names)
                    if not payload.get('title') and resource.title:
                        payload['title'] = resource.title
                    if payload.get('season') is None and resource.season is not None:
                        payload['season'] = resource.season
                    if not payload.get('episode_keys') and resource.episode_key:
                        payload['episode_keys'] = [resource.episode_key]

                cloud_cfg = None
                if provider not in ('dry-run', 'mock', 'test'):
                    cloud_cfg = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
                    if cloud_cfg is None:
                        raise RuntimeError(f'网盘提供商 {provider} 未配置')
                    if not cloud_cfg.enabled:
                        raise RuntimeError(f'网盘提供商 {provider} 已在配置中禁用')
                    if not payload.get('auth_token') and cloud_cfg.auth_ref:
                        payload['auth_token'] = cloud_cfg.auth_ref

                    if resource is not None:
                        if resource.tmdb_id and resource.season:
                            watchlist = await db.scalar(select(SeriesWatchlist).where(
                                SeriesWatchlist.tmdb_id == resource.tmdb_id,
                                SeriesWatchlist.season == resource.season,
                            ))
                        route = DestinationRouter.resolve(
                            resource=resource,
                            cloud_config=cloud_cfg,
                            watchlist=watchlist,
                            incoming_episode_keys=list(payload.get('episode_keys') or []),
                        )
                        payload['target_folder_id'] = route.target_folder_id
                        payload['destination_kind'] = route.kind
                        layout = _series_layout_names(resource, watchlist, route.kind)
                        if layout:
                            payload['series_folder_name'], payload['season_folder_name'] = layout
                            if (
                                route.kind == 'completed'
                                and watchlist is not None
                                and watchlist.remote_destination_kind == 'ongoing'
                                and watchlist.remote_series_folder_id
                            ):
                                payload['promotion_source_series_folder_id'] = watchlist.remote_series_folder_id
                    elif not payload.get('target_folder_id') and cloud_cfg.target_folder_id:
                        payload['target_folder_id'] = cloud_cfg.target_folder_id

                if is_promotion:
                    # A promotion moves an already verified remote root; it must never
                    # turn the resource's historical video list into a restore check.
                    payload['expected_files'] = []
                outcome = await self.orchestrator.execute(payload)
                transfer_result = {
                    'verified': outcome.verified,
                    'remote_folder_id': outcome.remote_folder_id,
                    'remote_files': list(outcome.remote_files),
                    'destination_kind': payload.get('destination_kind'),
                }
                if resource is not None and outcome.remote_folder_id:
                    resource.transferred_folder_id = outcome.remote_folder_id
                if watchlist is not None and outcome.remote_series_folder_id:
                    watchlist.remote_series_folder_id = outcome.remote_series_folder_id
                    watchlist.remote_destination_kind = outcome.remote_destination_kind or payload.get('destination_kind')
                await TransferQueueService.mark_completed(db, task, transfer_result)
                if not is_promotion and resource and resource.tmdb_id and resource.season:
                    keys = list(payload.get('episode_keys') or ([resource.episode_key] if resource.episode_key else []))
                    if keys:
                        await WatchlistService.mark_collected(db, tmdb_id=resource.tmdb_id, season=resource.season, episode_keys=keys)
                if self.notifier:
                    await self.notifier.notify_success(
                        task_payload=payload,
                        transfer_result=transfer_result,
                        resource=resource,
                    )
            except Exception as exc:
                logger.exception('transfer task %s failed', task.id)
                await TransferQueueService.mark_failed(db, task, str(exc))
                if self.notifier:
                    try:
                        await self.notifier.notify_failure(
                            task_payload=payload,
                            error_message=str(exc),
                            resource=resource,
                            attempts=getattr(task, 'attempt_count', 1),
                        )
                    except Exception as notif_err:  # noqa: BLE001
                        logger.warning('Failed to send failure notification for task %s: %s', task.id, notif_err)
        return True
