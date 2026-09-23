import logging
import os
import socket
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.follow.bot_settings_service import BotSettingsService
from app.follow.watchlist_service import WatchlistService
from app.models.cloud import CloudConfig
from app.models.resource import Resource, ResourceStatus
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.transfer.candidate_service import stable_share_key
from app.transfer.destination_routing import DestinationRouter
from app.transfer.errors import classify_error
from app.transfer.notifier import TransferNotifier
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.queue_service import TransferQueueService
from app.transfer.status import TransferStatus

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
    """Claim/execute/finalize in three separate transactions.

    Transaction A claims the task (status=RUNNING, attempt_count++, locks) and
    commits immediately. Remote IO then runs with NO open database transaction.
    Transaction B records verified success (COMPLETED + collected + resource
    status) or Transaction C records the classified failure. A database hiccup
    after a successful remote IO can therefore never roll the claim back into an
    un-claimed state and re-run the same restore.
    """

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

    # ------------------------------------------------------------------ #
    # Canary single-task execution (Phase 2C §十四/§十五)
    # ------------------------------------------------------------------ #

    async def execute_single_task(self, task_id: int, *, canary_override_pause: bool = False) -> dict:
        """Execute EXACTLY one task through the real production chain.

        ``canary_override_pause`` is deliberately available only on this
        explicit-id path.  It remains fail-closed unless its dedicated process
        has ``CANARY_CLOUD_WRITE_ENABLED=true``; ``run_forever`` and
        ``claim_next`` never read this flag or this environment variable.
        """
        async with self.session_factory() as db, db.begin():
            if await BotSettingsService.is_transfer_paused(db):
                canary_env = os.environ.get('CANARY_CLOUD_WRITE_ENABLED', '').strip().lower() in ('1', 'true', 'yes')
                if not canary_override_pause:
                    logger.info('Canary refused: transfer pause is enabled (transfer_paused=1)')
                    return {'executed': False, 'reason': 'transfer_paused=1'}
                if not canary_env:
                    logger.warning('Canary override refused: CANARY_CLOUD_WRITE_ENABLED is not true')
                    return {'executed': False, 'reason': 'canary_override_not_authorized'}
            task = await db.get(TransferQueueTask, task_id)
            if task is None:
                raise RuntimeError(f'task #{task_id} does not exist')
            if task.status not in ('QUEUED', 'RETRY_WAIT'):
                raise RuntimeError(f'task #{task_id} status is {task.status}; cannot canary')
            now = datetime.now(UTC)
            task.status = TransferStatus.RUNNING
            task.locked_at = now
            task.locked_by = self.worker_id
            task.attempt_count += 1
            payload = await self._hydrate_payload(db, task)
        try:
            outcome = await self.orchestrator.execute(payload)
        except Exception as exc:  # noqa: BLE001 - classified & recorded like the worker
            logger.warning('canary task %s failed (%s): %s', task_id, type(exc).__name__, exc)
            await self._record_failure(task_id, payload.get('resource_id'), payload, exc)
            return {'task_id': task_id, 'executed': True, 'success': False, 'error': str(exc)[:400]}
        await self._record_success(task_id, payload.get('resource_id'), payload, outcome)
        return {'task_id': task_id, 'executed': True, 'success': True,
                'verified': outcome.verified, 'remote_folder_id': outcome.remote_folder_id}

    async def _hydrate_payload(self, db: AsyncSession, task: TransferQueueTask) -> dict:
        """Shared hydration used by both claim_next and canary (Phase 2C §十四)."""
        payload = dict(task.payload or {})
        resource = await db.get(Resource, task.resource_id)
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
            payload['expected_files'] = []
        return payload

    # ------------------------------------------------------------------ #
    # Transaction A: claim
    # ------------------------------------------------------------------ #

    async def _claim_payload(self) -> tuple[int, int | None, dict] | None:
        """Claim one task and hydrate its payload; returns (task_id, resource_id, payload)."""
        async with self.session_factory() as db, db.begin():
            if await BotSettingsService.is_transfer_paused(db):
                logger.info('Transfer queue consumption skipped: transfer pause is enabled')
                return None
            task = await TransferQueueService.claim_next(db, worker_id=self.worker_id)
            if not task:
                return None
            payload = await self._hydrate_payload(db, task)
            return task.id, task.resource_id, payload

    # ------------------------------------------------------------------ #
    # Transaction B: verified success
    # ------------------------------------------------------------------ #

    async def _record_success(self, task_id: int, resource_id: int | None, payload: dict, outcome) -> bool:
        is_promotion = str(payload.get('operation') or '').strip().lower() == 'promote'
        async with self.session_factory() as db, db.begin():
            task = await db.get(TransferQueueTask, task_id)
            if task is None:
                logger.warning('transfer task %s disappeared before success recording', task_id)
                return True
            resource = await db.get(Resource, resource_id) if resource_id is not None else None
            watchlist = None
            transfer_result = {
                'verified': outcome.verified,
                'remote_folder_id': outcome.remote_folder_id,
                'remote_files': list(outcome.remote_files),
                'destination_kind': payload.get('destination_kind'),
            }
            if resource is not None and outcome.remote_folder_id:
                resource.transferred_folder_id = outcome.remote_folder_id
            if resource is not None and resource.tmdb_id and resource.season:
                watchlist = await db.scalar(select(SeriesWatchlist).where(
                    SeriesWatchlist.tmdb_id == resource.tmdb_id,
                    SeriesWatchlist.season == resource.season,
                ))
                if watchlist is not None and outcome.remote_series_folder_id:
                    watchlist.remote_series_folder_id = outcome.remote_series_folder_id
                    watchlist.remote_destination_kind = outcome.remote_destination_kind or payload.get('destination_kind')
            await TransferQueueService.mark_completed(db, task, transfer_result)
            if resource is not None and not is_promotion:
                resource.status = ResourceStatus.COMPLETED
            # Phase 2C §四/§六: a verified transfer retires the candidate.
            if resource is not None and not is_promotion and resource.tmdb_id and resource.season:
                from app.transfer.candidate_service import (
                    get_candidates_for_episode,
                    mark_candidate_transferred,
                )

                episode_keys = list(payload.get('episode_keys') or ([resource.episode_key] if resource.episode_key else []))
                for episode_key in episode_keys:
                    for candidate in await get_candidates_for_episode(
                        db,
                        tmdb_id=resource.tmdb_id,
                        season=resource.season,
                        episode_key=episode_key,
                    ):
                        if candidate.resource_id == resource.id or (
                            resource.share_url and candidate.share_hash == stable_share_key(resource.share_url)
                        ):
                            await mark_candidate_transferred(db, candidate=candidate)
            if not is_promotion and resource is not None and resource.tmdb_id and resource.season:
                keys = list(payload.get('episode_keys') or ([resource.episode_key] if resource.episode_key else []))
                if keys:
                    await WatchlistService.mark_collected(db, tmdb_id=resource.tmdb_id, season=resource.season, episode_keys=keys)
        if self.notifier:
            try:
                await self.notifier.notify_success(task_payload=payload, transfer_result=transfer_result, resource=resource)
            except Exception as notif_err:  # noqa: BLE001
                logger.warning('Failed to send success notification for task %s: %s', task_id, notif_err)
        return True

    # ------------------------------------------------------------------ #
    # Transaction C: classified failure
    # ------------------------------------------------------------------ #

    async def _record_failure(self, task_id: int, resource_id: int | None, payload: dict, exc: BaseException) -> bool:
        category = classify_error(exc)
        message = str(exc) or exc.__class__.__name__
        async with self.session_factory() as db, db.begin():
            task = await db.get(TransferQueueTask, task_id)
            if task is None:
                logger.warning('transfer task %s disappeared before failure recording', task_id)
                return True
            resource = await db.get(Resource, resource_id) if resource_id is not None else None
            await TransferQueueService.mark_failed(db, task, message, category=category)
            if resource is not None and task.status == TransferStatus.FAILED:
                resource.status = ResourceStatus.FAILED
            # Phase 2C §七: record the failure on the candidate ledger too, so
            # switch-resource can exclude permanently-dead candidates (and
            # never blacklist the resource for mere AUTH problems).
            await self._persist_failure_candidate(
                db,
                task=task,
                resource=resource,
                payload=payload,
                category=str(category) if category else None,
                failure_reason=message[:2000],
                task_id=task_id,
            )
        if self.notifier:
            try:
                await self.notifier.notify_failure(
                    task_payload=payload,
                    error_message=message,
                    resource=resource,
                    attempts=getattr(task, 'attempt_count', 1),
                    task_id=task_id,
                    category=str(category) if category else None,
                    stage='unknown',
                )
            except Exception as notif_err:  # noqa: BLE001
                logger.warning('Failed to send failure notification for task %s: %s', task_id, notif_err)
        return True

    async def _persist_failure_candidate(
        self,
        db: AsyncSession,
        *,
        task: TransferQueueTask,
        resource: Resource | None,
        payload: dict,
        category: str | None,
        failure_reason: str,
        task_id: int,
    ) -> None:
        """Map the classified failure onto the candidate ledger (Phase 2C §七)."""
        from app.transfer.candidate_service import (
            get_candidates_for_episode,
            mark_candidate_failure,
        )

        tmdb_id = payload.get('tmdb_id') or (resource.tmdb_id if resource else None)
        season = payload.get('season') or (resource.season if resource else None)
        episode_keys = list(payload.get('episode_keys') or ([resource.episode_key] if resource and resource.episode_key else []))
        share_url = payload.get('share_url') or (resource.share_url if resource else None)
        if not tmdb_id or not season or not episode_keys:
            return
        for episode_key in episode_keys:
            candidates = await get_candidates_for_episode(
                db,
                tmdb_id=int(tmdb_id),
                season=int(season),
                episode_key=episode_key,
            )
            target = None
            from app.transfer.candidate_service import stable_share_key

            for candidate in candidates:
                if share_url and candidate.share_hash == stable_share_key(str(share_url)):
                    target = candidate
                    break
            if target is None and resource is not None and resource.id is not None:
                resource_row_id = int(resource.id)
                for candidate in candidates:
                    if candidate.resource_id == resource_row_id:
                        target = candidate
                        break
            await mark_candidate_failure(
                db,
                candidate=target,
                category=category,
                failure_reason=f'{failure_reason} (task #{task_id})',
            )

    # ------------------------------------------------------------------ #
    # Orchestration
    # ------------------------------------------------------------------ #

    async def process_once(self) -> bool:
        claimed = await self._claim_payload()
        if claimed is None:
            return False
        task_id, resource_id, payload = claimed
        try:
            outcome = await self.orchestrator.execute(payload)
        except Exception as exc:  # noqa: BLE001 - every remote failure is classified & recorded
            logger.warning('transfer task %s failed (%s): %s', task_id, type(exc).__name__, exc)
            return await self._record_failure(task_id, resource_id, payload, exc)
        return await self._record_success(task_id, resource_id, payload, outcome)
