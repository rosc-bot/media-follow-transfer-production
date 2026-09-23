import asyncio
import logging
import os
import re
import socket
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.follow.bot_settings_service import BotSettingsService
from app.follow.episode_keys import canonical_episode_key
from app.follow.tmdb_provider import TMDBSeasonProvider
from app.follow.watchlist_service import WatchlistService
from app.models.cloud import CloudConfig, CloudDiskInventory
from app.models.resource import Resource, ResourceStatus
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.transfer.canary_preflight import preflight_task, validate_remote_canary
from app.transfer.candidate_service import stable_share_key
from app.transfer.canonical_destination import DestinationMetadataIncomplete
from app.transfer.cloud_inventory_service import CloudInventoryService
from app.transfer.destination_routing import DestinationRouter
from app.transfer.errors import (
    PromotionUnverifiedError,
    RenameUnverifiedError,
    TransferErrorCategory,
    classify_error,
)
from app.transfer.final_preflight import (
    AUTO_SAFE,
    build_source_rename_plan,
    classify_final_preflight,
)
from app.transfer.missing_episode_preflight import plan_missing_episode_transfer
from app.transfer.notifier import NotificationResult, TransferNotifier
from app.transfer.orchestrator import TransferOrchestrator
from app.transfer.provider_network_health import (
    complete_provider_health_probe,
    is_retryable_provider_failure,
    provider_backoff_seconds,
    provider_claim_gate,
    record_provider_network_failure,
)
from app.transfer.queue_service import TransferQueueService
from app.transfer.status import EXECUTION_ACTIVE_STATUSES, REVIEW_STATUS, TransferStatus
from app.transfer.task_identity import task_matches_episode

logger = logging.getLogger(__name__)

_SERIES_MEDIA_TYPES = frozenset({'tv', 'anime', 'series', '电视剧', '动漫'})
_ACTIVE_EPISODE_TASK_STATUSES = EXECUTION_ACTIVE_STATUSES
BATCH_PREFLIGHT_TIMEOUT_SECONDS = 90.0
_SENSITIVE_VALUE_RE = re.compile(
    r'(?i)\b(access[_-]?token|refresh[_-]?token|authorization|cookie|auth_ref|password|secret|api[_-]?key)\b(\s*[:=]\s*)[^\s,;]+'
)
_BEARER_VALUE_RE = re.compile(r'(?i)\bBearer\s+[^\s,;]+')
_URL_QUERY_RE = re.compile(r'(https?://[^\s?#]+)\?[^\s#]+')


def _safe_preflight_detail(exc: BaseException) -> str:
    message = str(exc)
    message = _URL_QUERY_RE.sub(r'\1?[REDACTED_QUERY]', message)
    message = _SENSITIVE_VALUE_RE.sub(r'\1\2[REDACTED]', message)
    message = _BEARER_VALUE_RE.sub('Bearer [REDACTED]', message)
    return f'{type(exc).__name__}: {message[:600]}'


def _is_active_episode_task_status(status: object) -> bool:
    return str(status or '').strip().upper() in _ACTIVE_EPISODE_TASK_STATUSES


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
    season = int(resource.season or getattr(watchlist, 'season', 1) or 1)
    if season < 1:
        raise RuntimeError('series transfer requires a positive season number')
    return series_name, f'S{season:02d}'


def _metadata_relevant_seasons(metadata: dict, fallback_season: int) -> list[int]:
    seasons: set[int] = set()
    for item in metadata.get('seasons') or []:
        if not isinstance(item, dict):
            continue
        try:
            season = int(item.get('season_number') or 0)
        except (TypeError, ValueError):
            continue
        if season > 0:
            seasons.add(season)
    return sorted(seasons) or [int(fallback_season)]


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

    async def _provider_health_probe(self) -> bool:
        """Perform one bounded, read-only direct-directory probe for Guangya."""
        from app.transfer.adapters.guangya import GuangyaAdapter
        from app.transfer.guangya_auth import GuangyaCredentialStore

        async with self.session_factory() as db:
            config = await db.scalar(select(CloudConfig).where(CloudConfig.name == 'guangya'))
            if config is None or not config.enabled or not config.auth_ref:
                logger.warning('Guangya provider health probe unavailable: credentials/config missing')
                return False
            auth_ref = str(config.auth_ref)
            root_id = str(config.ongoing_target_folder_id or config.target_folder_id or '').strip()
        if not root_id:
            logger.warning('Guangya provider health probe unavailable: target root missing')
            return False
        adapter = GuangyaAdapter(
            write_enabled=False,
            credential_store=GuangyaCredentialStore(self.session_factory),
        )
        try:
            await asyncio.wait_for(
                adapter.list_directories(auth_token=auth_ref, parent_id=root_id),
                timeout=35.0,
            )
        except Exception as exc:  # noqa: BLE001 - probe fails closed, no cloud write path exists
            logger.warning('Guangya read-only health probe failed: %s', _safe_preflight_detail(exc))
            return False
        logger.info('Guangya read-only health probe succeeded: directory_read_success=true')
        return True

    async def _provider_claim_allowed(self) -> bool:
        """Pause only new claims during provider cooldown; recover via read-only probe."""
        async with self.session_factory() as db, db.begin():
            gate = await provider_claim_gate(db)
        if gate == 'ALLOW':
            return True
        if gate == 'WAIT':
            return False
        probe_ok = await self._provider_health_probe()
        async with self.session_factory() as db, db.begin():
            await complete_provider_health_probe(db, success=probe_ok)
        if probe_ok:
            logger.info('Guangya provider circuit closed after successful read-only probe')
        return probe_ok

    async def _defer_provider_network_failure(
        self,
        db: AsyncSession,
        *,
        task: TransferQueueTask,
        payload: dict,
        reason: str,
        stage: str,
        detail: object = '',
    ) -> None:
        """Release a preflight-only network failure into bounded durable retry."""
        safe_detail = _safe_preflight_detail(RuntimeError(str(detail))) if detail else reason
        network_attempts = int((task.payload or {}).get('preflight_network_attempts') or payload.get('preflight_network_attempts') or 0) + 1
        now = datetime.now(UTC)
        retry_at = now + timedelta(seconds=provider_backoff_seconds(network_attempts))
        task.status = TransferStatus.RETRY_WAIT
        task.next_run_at = retry_at
        # A preflight failure has performed no business transfer attempt.
        task.attempt_count = max(0, int(task.attempt_count or 0) - 1)
        task.error_message = f'[PROVIDER_NETWORK_RETRY] stage={stage} reason={reason} {safe_detail}'[:4000]
        task.locked_at = None
        task.locked_by = None
        merged_payload = {**dict(task.payload or {}), **payload}
        merged_payload.pop('preflight_classification', None)
        merged_payload.update({
            'preflight_reason': 'PROVIDER_NETWORK_TIMEOUT' if 'TIMEOUT' in reason.upper() else 'PROVIDER_NETWORK_ERROR',
            'preflight_stage': stage,
            'preflight_detail': safe_detail[:800],
            'preflight_network_attempts': network_attempts,
            'preflight_retry_at': retry_at.isoformat(),
        })
        task.payload = merged_payload
        provider = str(payload.get('provider') or '').strip().casefold()
        if provider == 'guangya':
            breaker = await record_provider_network_failure(db, task_id=int(task.id))
            if breaker['state'] == 'PROVIDER_DEGRADED' and breaker.get('cooldown_until'):
                cooldown = datetime.fromisoformat(str(breaker['cooldown_until']))
                if cooldown.tzinfo is None:
                    cooldown = cooldown.replace(tzinfo=UTC)
                task.next_run_at = max(retry_at, cooldown)
                task.payload['preflight_retry_at'] = task.next_run_at.isoformat()
        await db.flush()
        logger.warning(
            'Transfer task %s deferred without global pause: network preflight stage=%s reason=%s retry_at=%s',
            task.id, stage, reason, task.next_run_at.isoformat(),
        )

    # ------------------------------------------------------------------ #
    # Canary single-task execution (Phase 2C §十四/§十五)
    # ------------------------------------------------------------------ #

    async def execute_single_task(
        self,
        task_id: int,
        *,
        canary_override_pause: bool = False,
        selection_snapshot: dict | None = None,
    ) -> dict:
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
            if selection_snapshot:
                snapshot = dict(selection_snapshot)
                payload['selection_mode'] = snapshot.get('selection_mode') or payload.get('selection_mode')
                payload['selected_file_ids'] = list(snapshot.get('selected_file_ids') or [])
                payload['selected_file_names'] = list(snapshot.get('selected_file_names') or [])
                payload['expected_files'] = list(snapshot.get('selected_file_names') or payload.get('expected_files') or [])
                payload['selection_snapshot'] = snapshot
                task.payload = {
                    **dict(task.payload or {}),
                    'selection_mode': payload['selection_mode'],
                    'selected_file_ids': payload['selected_file_ids'],
                    'selected_file_names': payload['selected_file_names'],
                    'expected_files': payload['expected_files'],
                    'selection_snapshot': snapshot,
                }
        try:
            outcome = await self.orchestrator.execute(payload)
        except Exception as exc:  # noqa: BLE001 - classified & recorded like the worker
            logger.warning('canary task %s failed (%s): %s', task_id, type(exc).__name__, exc)
            await self._record_failure(task_id, payload.get('resource_id'), payload, exc)
            return {'task_id': task_id, 'executed': True, 'success': False, 'error': str(exc)[:400]}
        await self._record_success(task_id, payload.get('resource_id'), payload, outcome)
        return {'task_id': task_id, 'executed': True, 'success': True,
                'verified': outcome.verified, 'remote_folder_id': outcome.remote_folder_id}

    async def _load_tmdb_metadata(self, payload: dict, resource: Resource) -> dict:
        cached = payload.get('tmdb_metadata') or payload.get('tmdb_details')
        media_kind = 'movie' if str(resource.media_type or 'tv').casefold() in {'movie', 'film', '电影'} else 'tv'
        if isinstance(cached, dict) and cached.get('id'):
            cached_kind = str(cached.get('media_type') or media_kind).casefold()
            cached_seasons = cached.get('seasons')
            has_relevant_seasons = isinstance(cached_seasons, list) and any(
                isinstance(item, dict) and str(item.get('season_number') or '').isdigit()
                and int(item.get('season_number') or 0) > 0
                for item in cached_seasons
            )
            if cached_kind == media_kind and (media_kind == 'movie' or has_relevant_seasons):
                return dict(cached)
        if not resource.tmdb_id:
            raise DestinationMetadataIncomplete('tmdb_id is required before destination routing')
        settings = get_settings()
        provider = TMDBSeasonProvider(
            api_key=settings.tmdb_api_key,
            base_url=settings.tmdb_base_url,
        )
        try:
            metadata = await provider.fetch_details(int(resource.tmdb_id), str(resource.media_type or 'tv'))
        except Exception as exc:
            raise DestinationMetadataIncomplete(f'TMDB metadata unavailable: {type(exc).__name__}') from exc
        if not isinstance(metadata, dict) or not metadata.get('id'):
            raise DestinationMetadataIncomplete('TMDB metadata response is incomplete')
        payload['tmdb_metadata'] = metadata
        return metadata

    async def _hydrate_payload(self, db: AsyncSession, task: TransferQueueTask) -> dict:
        """Shared hydration used by both claim_next and canary (Phase 2C §十四)."""
        payload = dict(task.payload or {})
        resource = await db.get(Resource, task.resource_id)
        is_promotion = str(payload.get('operation') or '').strip().lower() == 'promote'
        provider = str(payload.get('provider') or (resource.cloud_name if resource else '') or 'guangya').lower()
        payload['provider'] = provider
        payload['success_notification_chat'] = await BotSettingsService.get(
            db,
            'transfer_success_chat',
            get_settings().transfer_success_chat,
        )
        payload['resource_publish_chat'] = await BotSettingsService.get(
            db,
            'resource_publish_chat',
            get_settings().resource_publish_chat,
        )
        watchlist = None

        if resource:
            payload.setdefault('media_type', resource.media_type)
            payload.setdefault('source_type', resource.source_type)
            payload.setdefault('source_channel_id', resource.source_channel_id)
            payload.setdefault('source_message_id', resource.source_message_id)
            if not payload.get('share_url') and resource.share_url:
                payload['share_url'] = resource.share_url
            if not payload.get('expected_files') and resource.file_names:
                payload['expected_files'] = list(resource.file_names)
            if not payload.get('title') and resource.title:
                payload['title'] = resource.title
            if payload.get('tmdb_id') is None and resource.tmdb_id is not None:
                payload['tmdb_id'] = resource.tmdb_id
            if payload.get('season') is None and resource.season is not None:
                payload['season'] = resource.season
            if not payload.get('episode_keys') and resource.episode_key:
                payload['episode_keys'] = [resource.episode_key]
            if str(resource.media_type or 'tv').casefold() in _SERIES_MEDIA_TYPES and not is_promotion:
                selected_mode = str(payload.get('selection_mode') or '').strip().upper()
                explicit_mode = bool(payload.get('selection_mode_explicit'))
                authorized_whole = bool(payload.get('whole_share_authorized'))
                if selected_mode == 'WHOLE_SHARE' and explicit_mode and authorized_whole:
                    payload['selection_mode'] = 'WHOLE_SHARE'
                elif selected_mode in {'SINGLE_EPISODE', 'COLLECTION'} and explicit_mode:
                    payload['selection_mode'] = selected_mode
                else:
                    payload['selection_mode'] = 'MISSING_EPISODES'
                    payload['selection_mode_source'] = 'AUTO_MISSING_EPISODE_RECONCILIATION'
            elif not payload.get('selection_mode') and payload.get('episode_keys'):
                payload['selection_mode'] = 'SINGLE_EPISODE'

        if provider not in ('dry-run', 'mock', 'test'):
            cloud_cfg = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
            if cloud_cfg is None:
                raise RuntimeError(f'网盘提供商 {provider} 未配置')
            if not cloud_cfg.enabled:
                raise RuntimeError(f'网盘提供商 {provider} 已在配置中禁用')
            payload['ongoing_root_id'] = str(cloud_cfg.ongoing_target_folder_id or '').strip()
            payload['completed_root_id'] = str(cloud_cfg.target_folder_id or '').strip()
            if not payload.get('auth_token') and cloud_cfg.auth_ref:
                payload['auth_token'] = cloud_cfg.auth_ref

            if resource is not None:
                metadata = None
                if resource.tmdb_id and resource.season:
                    watchlist = await db.scalar(select(SeriesWatchlist).where(
                        SeriesWatchlist.tmdb_id == resource.tmdb_id,
                        SeriesWatchlist.season == resource.season,
                    ))
                    if watchlist is not None:
                        payload.setdefault('poster_url', watchlist.poster_path)
                        payload.setdefault('watchlist_poster_path', watchlist.poster_path)
                        payload.setdefault('series_status', watchlist.tmdb_series_status)
                        payload.setdefault('tmdb_series_status', watchlist.tmdb_series_status)
                        payload.setdefault('total_episodes', watchlist.total_episodes)
                        payload.setdefault('last_aired_episode', watchlist.last_aired_episode)
                        payload.setdefault('collected_episodes', list(watchlist.collected_episodes or []))
                        payload.setdefault('subscriber_tg_id', watchlist.subscriber_tg_id)
                        payload.setdefault('source', watchlist.source)
                        payload.setdefault('year', watchlist.year or resource.year)
                        if payload.get('notification_chat_id') is None and watchlist.subscriber_tg_id is not None:
                            payload['notification_chat_id'] = watchlist.subscriber_tg_id
                            payload['watchlist_subscriber_tg_id'] = watchlist.subscriber_tg_id
                    if (
                        watchlist is not None
                        and payload.get('notification_chat_id') is None
                        and watchlist.subscriber_tg_id is not None
                    ):
                        payload['notification_chat_id'] = watchlist.subscriber_tg_id
                        payload['watchlist_subscriber_tg_id'] = watchlist.subscriber_tg_id
                if resource.tmdb_id:
                    metadata = await self._load_tmdb_metadata(payload, resource)
                    if str(resource.media_type or 'tv').casefold() in _SERIES_MEDIA_TYPES:
                        authoritative_status = str(metadata.get('status') or '').strip()
                        if authoritative_status:
                            payload['tmdb_series_status'] = authoritative_status
                            payload['series_status'] = authoritative_status
                    route = DestinationRouter.resolve(
                        resource=resource,
                        cloud_config=cloud_cfg,
                        watchlist=watchlist,
                        incoming_episode_keys=list(payload.get('episode_keys') or []),
                        metadata=metadata,
                        operation='promote' if is_promotion else 'transfer',
                        physical_complete=is_promotion,
                    )
                    payload['target_folder_id'] = route.target_folder_id
                    payload['destination_kind'] = route.kind
                    if route.destination is not None:
                        destination = route.destination
                        payload.update({
                            'media_root': destination.media_root,
                            'media_category': destination.media_category,
                            'sub_category': destination.media_category,
                            'series_folder_name': destination.item_name,
                            'season_folder_name': destination.season_name,
                            'destination_prefix': destination.relative_item_path,
                            'inventory_prefix': destination.inventory_prefix,
                            'remote_rel_path_prefix': destination.inventory_prefix,
                            'archive_directory': destination.archive_directory,
                            'category_resolution': destination.resolution.as_dict(),
                            'season_layout_mode': 'MULTI_SEASON' if destination.season_name else 'SINGLE_SEASON_FLAT',
                            'relevant_seasons': _metadata_relevant_seasons(metadata, int(resource.season or 1)),
                        })
                    else:
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

    async def _final_local_preflight(self, db: AsyncSession, task: TransferQueueTask, payload: dict) -> tuple[str, str] | None:
        """Run non-provider duplicate/collected/inventory gates before remote IO."""
        if str(payload.get('provider') or '').casefold() in {'dry-run', 'mock', 'test'}:
            return None
        classification = str(payload.get('preflight_classification') or '').strip().upper()
        if classification in {'NEEDS_REVIEW', 'REJECTED'}:
            return classification, f'PREFLIGHT_CLASSIFICATION={classification}'
        if str(payload.get('operation') or '').strip().lower() == 'promote':
            return None
        resource = await db.get(Resource, task.resource_id) if task.resource_id is not None else None
        if resource is None or not resource.tmdb_id or not resource.season:
            return 'REJECTED', 'IDENTITY_MISSING'
        episode_keys = list(payload.get('episode_keys') or ([resource.episode_key] if resource.episode_key else []))
        canonical_keys = {
            key for value in episode_keys
            if (key := canonical_episode_key(resource.season, value)) is not None
        }
        if not canonical_keys:
            return 'REJECTED', 'EPISODE_IDENTITY_MISSING'
        watchlists = list((await db.scalars(select(SeriesWatchlist).where(
            SeriesWatchlist.tmdb_id == resource.tmdb_id,
            SeriesWatchlist.season == resource.season,
            SeriesWatchlist.status != 'CANCELLED',
        ))).all())
        if len(watchlists) != 1:
            return 'REJECTED', f'WATCHLIST_MATCH_COUNT={len(watchlists)}'
        if str(payload.get('selection_mode') or '').upper() == 'MISSING_EPISODES':
            # Share-wide episode/presence reconciliation runs in the provider-aware
            # runtime preflight after claim; the trigger episode may already be
            # collected while other files in its share are still missing.
            return None
        collected = {
            key for value in (watchlists[0].collected_episodes or [])
            if (key := canonical_episode_key(resource.season, value)) is not None
        }
        overlap = sorted(collected & canonical_keys)
        if overlap:
            return 'REJECTED', f'ALREADY_COLLECTED={overlap}'
        episode_numbers = {
            int(key.split('E', 1)[1])
            for key in canonical_keys
            if 'E' in key and key.split('E', 1)[1].isdigit()
        }
        inventory = list((await db.scalars(select(CloudDiskInventory).where(
            CloudDiskInventory.tmdb_id == resource.tmdb_id,
            CloudDiskInventory.season == resource.season,
        ))).all())
        occupied = sorted(episode_numbers & {int(row.episode) for row in inventory if row.episode is not None})
        if occupied:
            return 'REJECTED', f'ALREADY_IN_CLOUD={occupied}'
        active_statuses = EXECUTION_ACTIVE_STATUSES
        tasks = list((await db.scalars(select(TransferQueueTask).where(TransferQueueTask.id != task.id))).all())
        resource_ids = {row.resource_id for row in tasks if row.resource_id is not None}
        related_resources = {
            row.id: row
            for row in (await db.scalars(select(Resource).where(Resource.id.in_(resource_ids)))).all()
        } if resource_ids else {}
        for other in tasks:
            if not task_matches_episode(
                other.payload,
                tmdb_id=int(resource.tmdb_id),
                season=int(resource.season),
                episode_keys=canonical_keys,
                resource=related_resources.get(other.resource_id),
            ):
                continue
            if str(other.status) in {'SUCCESS', 'COMPLETED'}:
                return 'REJECTED', f'DUPLICATE_SUCCESS_TASK={other.id}'
            if str(other.status) in active_statuses:
                return 'NEEDS_REVIEW', f'DUPLICATE_ACTIVE_TASK={other.id}'
        return None

    async def _reconcile_verified_cloud_presence(
        self,
        *,
        tmdb_id: int,
        season: int,
        title: str,
        watchlist_id: int,
        series_root_id: str,
        inventory_prefix: str | None,
        metadata_reconcile: dict[str, list[str] | tuple[str, ...]],
        cloud_files: list[dict],
        provider: str,
    ) -> dict[str, int]:
        """Apply ledger-only repairs in a separate transaction from preflight."""
        if not metadata_reconcile:
            return {"inventory_added": 0, "inventory_updated": 0, "collected_added": 0}
        by_episode: dict[str, list[dict]] = {}
        for record in cloud_files:
            key = str(record.get("episode_key") or "").strip().upper()
            if key:
                by_episode.setdefault(key, []).append(record)
        evidence: dict[str, dict] = {}
        for key in metadata_reconcile:
            records = by_episode.get(key, [])
            if len(records) != 1:
                raise RuntimeError(f"verified cloud evidence is not unique for {key}")
            evidence[key] = records[0]

        inventory_added = 0
        inventory_updated = 0
        collected_added = 0
        async with self.session_factory() as reconcile_db, reconcile_db.begin():
            watchlist = await reconcile_db.scalar(
                select(SeriesWatchlist)
                .where(SeriesWatchlist.id == int(watchlist_id))
                .with_for_update()
            )
            if watchlist is None:
                raise RuntimeError("watchlist disappeared during cloud metadata reconciliation")
            for key, ledgers in metadata_reconcile.items():
                record = evidence[key]
                file_name = str(record.get("name") or "").strip()
                cloud_path = str(record.get("path") or file_name).strip("/")
                rel_path = "/".join(
                    part for part in (str(inventory_prefix or "").strip("/"), cloud_path) if part
                ) or None
                if "inventory" in ledgers:
                    result = await CloudInventoryService.upsert_verified_transfer(
                        reconcile_db,
                        tmdb_id=int(tmdb_id),
                        title=title,
                        season=int(season),
                        episode_key=key,
                        file_name=file_name,
                        verified=True,
                        rel_path=rel_path,
                        remote_file_id=str(record.get("file_id") or "") or None,
                        remote_folder_id=str(series_root_id or "") or None,
                        provider=provider,
                        source="verified_cloud_preflight_reconcile",
                        scanned_at=datetime.now(UTC),
                    )
                    inventory_added += int(result.status == "INSERTED")
                    inventory_updated += int(result.status == "UPDATED")
                    if not result.persisted:
                        raise RuntimeError(f"verified inventory reconciliation refused for {key}: {result.reason}")
                if "collected" in ledgers:
                    current = list(watchlist.collected_episodes or [])
                    canonical = {
                        value
                        for raw in current
                        if (value := canonical_episode_key(int(season), raw)) is not None
                    }
                    if key not in canonical:
                        current.append(key)
                        watchlist.collected_episodes = current
                        collected_added += 1
            await reconcile_db.flush()
        return {
            "inventory_added": inventory_added,
            "inventory_updated": inventory_updated,
            "collected_added": collected_added,
        }

    async def _recover_stale_pending_after_final_preflight(
        self,
        db: AsyncSession,
        *,
        current_task: TransferQueueTask,
        resource: Resource,
        payload: dict,
        missing_episode_keys: list[str],
    ) -> dict[str, int | bool]:
        """Recover only identity-matched review rows after a fresh AUTO_SAFE gate."""
        if not resource.tmdb_id or not resource.season:
            return {"queued_reactivated": 0, "stale_pending_recovered": 0, "reconciled": 0}
        batch_evidence = payload.get('batch_presence_preflight') or {}
        if not batch_evidence.get('cloud_scan_verified'):
            return {"queued_reactivated": 0, "stale_pending_recovered": 0, "reconciled": 0}
        decisions = batch_evidence.get('presence_decisions') or {}
        cloud_present = {
            str(key)
            for key, value in decisions.items()
            if isinstance(value, dict) and value.get('classification') == 'PRESENT_CONFIRMED'
        }
        missing = {
            key
            for value in missing_episode_keys
            if (key := canonical_episode_key(int(resource.season), value)) is not None
        }
        resource_ids = list((await db.scalars(
            select(Resource.id).where(
                Resource.tmdb_id == int(resource.tmdb_id),
                Resource.season == int(resource.season),
            )
        )).all())
        if not resource_ids:
            return {"queued_reactivated": 0, "stale_pending_recovered": 0, "reconciled": 0}
        pending_tasks = list((await db.scalars(
            select(TransferQueueTask)
            .where(
                TransferQueueTask.status == REVIEW_STATUS,
                TransferQueueTask.id != current_task.id,
                TransferQueueTask.resource_id.in_(resource_ids),
            )
            .order_by(TransferQueueTask.id.asc())
        )).all())
        reconciled = 0
        for review_task in pending_tasks:
            review_payload = dict(review_task.payload or {})
            review_resource = await db.get(Resource, review_task.resource_id) if review_task.resource_id else None
            old_keys = {
                key
                for value in (
                    review_payload.get('episode_keys')
                    or ([review_resource.episode_key] if review_resource and review_resource.episode_key else [])
                )
                if (key := canonical_episode_key(int(resource.season), value)) is not None
            }
            if old_keys and old_keys.issubset(cloud_present):
                review_task.error_message = '[FINAL_PREFLIGHT:REJECTED] RECONCILED_ALREADY_IN_CLOUD'
                review_task.payload = {
                    **review_payload,
                    'preflight_classification': 'REJECTED',
                    'preflight_reason': 'RECONCILED_ALREADY_IN_CLOUD',
                    'preflight_stage': 'FINAL_DECISION',
                    'stale_pending_recovered': True,
                }
                review_task.locked_at = None
                review_task.locked_by = None
                reconciled += 1
                continue
            if not old_keys or not old_keys.issubset(missing) or review_resource is None:
                continue
            old_share = str(review_payload.get('share_url') or review_resource.share_url or '')
            new_share = str(payload.get('share_url') or resource.share_url or '')
            if not old_share or not new_share or stable_share_key(old_share) == stable_share_key(new_share):
                continue

            recovered_keys = sorted(missing)
            review_task.resource_id = resource.id
            review_task.status = TransferStatus.QUEUED
            review_task.next_run_at = datetime.now(UTC)
            review_task.locked_at = None
            review_task.locked_by = None
            review_task.error_message = None
            review_task.idempotency_key = f'{resource.id}:reactivated:{review_task.id}'
            review_task.payload = {
                **dict(current_task.payload or {}),
                'resource_id': resource.id,
                'share_url': new_share,
                'tmdb_id': int(resource.tmdb_id),
                'title': resource.title or payload.get('title'),
                'season': int(resource.season),
                'episode_keys': recovered_keys,
                'selected_episode_keys': recovered_keys,
                'selection_mode': 'MISSING_EPISODES',
                'preflight_classification': AUTO_SAFE,
                'preflight_reason': 'STALE_PENDING_RECOVERED_FROM_NEW_CANDIDATE',
                'preflight_stage': 'FINAL_DECISION',
                'queued_reactivated': True,
                'stale_pending_recovered': True,
                'candidate_switched': True,
                'batch_presence_preflight': batch_evidence,
            }
            current_task.status = 'CANCELLED'
            current_task.error_message = f'[STALE_PENDING_RECOVERED] superseded by task {review_task.id}'
            current_task.locked_at = None
            current_task.locked_by = None
            current_task.payload = {
                **dict(current_task.payload or {}),
                'preflight_classification': 'REJECTED',
                'preflight_reason': 'STALE_PENDING_RECOVERED',
                'stale_pending_recovered_task_id': review_task.id,
            }
            from app.models.resource_candidate import ResourceCandidate
            from app.transfer.candidate_service import mark_candidate_used

            candidates = list((await db.scalars(
                select(ResourceCandidate).where(
                    ResourceCandidate.tmdb_id == int(resource.tmdb_id),
                    ResourceCandidate.season == int(resource.season),
                    ResourceCandidate.episode_key.in_(recovered_keys),
                    ResourceCandidate.share_hash == stable_share_key(new_share),
                )
            )).all())
            for candidate in candidates:
                candidate.resource_id = resource.id
                await mark_candidate_used(db, candidate=candidate, queue_task_id=review_task.id)
            await db.flush()
            return {
                'queued_reactivated': 1,
                'stale_pending_recovered': 1,
                'reconciled': reconciled,
                'reactivated_task_id': review_task.id,
            }
        return {
            "queued_reactivated": 0,
            "stale_pending_recovered": int(reconciled > 0),
            "reconciled": reconciled,
        }

    async def _batch_episode_presence_preflight(
        self,
        db: AsyncSession,
        *,
        task: TransferQueueTask,
        resource: Resource,
        payload: dict,
    ) -> dict:
        """Reconcile share, collected, DB inventory and live cloud before selecting missing episodes."""
        from app.transfer.adapters.guangya import GuangyaAdapter

        tmdb_id = int(resource.tmdb_id or 0) if resource is not None else 0
        season = int(resource.season or 0) if resource is not None else 0
        if tmdb_id <= 0 or season <= 0 or not resource or not resource.share_url:
            return {'classification': 'REJECTED', 'reason': 'BATCH_IDENTITY_OR_SHARE_MISSING'}
        payload['preflight_stage'] = 'COLLECTED_READ'
        watchlists = list((await db.scalars(select(SeriesWatchlist).where(
            SeriesWatchlist.tmdb_id == tmdb_id,
            SeriesWatchlist.season == season,
            SeriesWatchlist.status != 'CANCELLED',
        ).order_by(SeriesWatchlist.id.asc()))).all())
        if len(watchlists) != 1:
            return {'classification': 'NEEDS_REVIEW', 'reason': f'WATCHLIST_MATCH_COUNT={len(watchlists)}'}
        watchlist = watchlists[0]
        cloud_cfg = await db.scalar(select(CloudConfig).where(
            CloudConfig.name == str(payload.get('provider') or resource.cloud_name or 'guangya').casefold()
        ))
        if cloud_cfg is None or not cloud_cfg.enabled or not cloud_cfg.auth_ref:
            return {'classification': 'NEEDS_REVIEW', 'reason': 'CLOUD_PROVIDER_AUTH_UNAVAILABLE'}
        if not payload.get('media_root') or not payload.get('media_category') or not payload.get('target_folder_id'):
            return {'classification': 'NEEDS_REVIEW', 'reason': 'CANONICAL_DESTINATION_INCOMPLETE'}

        adapter = GuangyaAdapter(write_enabled=False)
        payload['preflight_stage'] = 'DESTINATION_LOOKUP'
        root_report = await adapter.inspect_tmdb_series_root_readonly(
            auth_token=str(cloud_cfg.auth_ref),
            target_root_id=str(payload['target_folder_id']),
            media_root_name=str(payload['media_root']),
            media_category_name=str(payload['media_category']),
            tmdb_id=tmdb_id,
            expected_series_name=str(payload.get('series_folder_name') or ''),
        )
        if root_report.get('status') != 'VERIFIED':
            return {
                'classification': 'NEEDS_REVIEW',
                'reason': 'DESTINATION_LOOKUP_API_ERROR' if root_report.get('status') == 'API_ERROR' else str(root_report.get('error') or root_report.get('status') or 'SERIES_ROOT_UNVERIFIED'),
                'detail': str(root_report.get('error') or root_report.get('status') or 'SERIES_ROOT_UNVERIFIED'),
                'preflight_stage': 'DESTINATION_LOOKUP',
            }
        roots = list(root_report.get('series_roots') or [])
        if len(roots) > 1:
            return {'classification': 'NEEDS_REVIEW', 'reason': 'DUPLICATE_TMDB_ROOT'}
        root_id = str(roots[0].get('folder_id') or '') if roots else ''
        if watchlist.remote_series_folder_id and root_id and str(watchlist.remote_series_folder_id) != root_id:
            return {'classification': 'NEEDS_REVIEW', 'reason': 'WATCHLIST_SERIES_ROOT_MISMATCH'}
        if watchlist.remote_series_folder_id and not root_id:
            return {'classification': 'NEEDS_REVIEW', 'reason': 'WATCHLIST_SERIES_ROOT_MISSING'}

        cloud_keys: set[str] = set()
        cloud_files: list[dict] = []
        cloud_scan = None
        cloud_scan_verified = not root_id
        cloud_pagination_complete = not root_id
        if root_id:
            relevant_seasons = list(payload.get('relevant_seasons') or [season])
            cloud_scan = None
            for attempt in range(3):
                payload['preflight_stage'] = 'CLOUD_PRESENCE_SCAN'
                cloud_scan = await adapter.scan_series_root_readonly(
                    auth_token=str(cloud_cfg.auth_ref),
                    tmdb_id=tmdb_id,
                    series_root_id=root_id,
                    relevant_seasons=relevant_seasons,
                    timeout_seconds=12,
                    max_depth=6,
                    max_items=5000,
                    page_size=100,
                    rate_limit_seconds=0.05,
                )
                if cloud_scan.get('scan_status') == 'VERIFIED' and not cloud_scan.get('truncated'):
                    cloud_scan_verified = True
                    cloud_pagination_complete = True
                    break
                if attempt < 2 and cloud_scan.get('scan_status') in {'API_ERROR', 'TIMEOUT_UNVERIFIED'}:
                    await asyncio.sleep((1.0, 3.0)[attempt])
                else:
                    break
            if not cloud_scan_verified:
                scan_status = str((cloud_scan or {}).get('scan_status') or 'UNVERIFIED')
                scan_error = str((cloud_scan or {}).get('error') or '')
                return {
                    'classification': 'NEEDS_REVIEW',
                    'reason': f'PHYSICAL_CLOUD_SCAN_{scan_status}',
                    'detail': scan_error,
                    'preflight_stage': 'CLOUD_PRESENCE_SCAN',
                }
            cloud_keys = {
                str(value)
                for value in (cloud_scan.get('cloud_episode_keys_by_season') or {}).get(str(season), [])
            }
            cloud_files = [
                dict(item)
                for item in cloud_scan.get('verified_files') or []
                if isinstance(item, dict)
            ]
            files_by_key: dict[str, list[dict]] = {}
            for item in cloud_files:
                key = str(item.get('episode_key') or '').strip().upper()
                if key:
                    files_by_key.setdefault(key, []).append(item)
            for key, records in files_by_key.items():
                identities = {
                    (str(record.get('file_id') or ''), str(record.get('name') or ''), str(record.get('path') or ''))
                    for record in records
                }
                if len(identities) > 1:
                    return {
                        'classification': 'NEEDS_REVIEW',
                        'reason': f'CLOUD_MULTIVERSION_CONFLICT:{key}',
                        'preflight_stage': 'CLOUD_PRESENCE_SCAN',
                    }
            cloud_files = [records[0] for records in files_by_key.values()]

        share_listing = None
        for attempt in range(3):
            payload['preflight_stage'] = 'SHARE_PROBE'
            try:
                share_listing = await adapter.inspect_share(
                    share_url=str(resource.share_url),
                    max_depth=6,
                    max_items=5000,
                    max_pages=100,
                )
                break
            except Exception as exc:  # only bounded transient retry; all other errors fail closed
                category = classify_error(exc)
                if category not in {
                    TransferErrorCategory.NETWORK_TIMEOUT,
                    TransferErrorCategory.NETWORK_ERROR,
                    TransferErrorCategory.REMOTE_5XX,
                    TransferErrorCategory.RATE_LIMITED,
                }:
                    raise
                if attempt == 2:
                    return {
                        'classification': 'NEEDS_REVIEW',
                        'reason': str(category),
                        'detail': _safe_preflight_detail(exc),
                        'preflight_stage': 'SHARE_PROBE',
                    }
                await asyncio.sleep((1.0, 3.0)[attempt])
        if not share_listing or not share_listing.get('share_readable') or share_listing.get('truncated'):
            return {'classification': 'NEEDS_REVIEW', 'reason': 'SHARE_UNREADABLE_OR_TRUNCATED'}
        payload['preflight_stage'] = 'INVENTORY_READ'
        inventory_rows = list((await db.scalars(select(CloudDiskInventory).where(
            CloudDiskInventory.tmdb_id == tmdb_id,
            CloudDiskInventory.season == season,
        ))).all())
        inventory_keys = {f'S{season:02d}E{int(row.episode):02d}' for row in inventory_rows}

        payload['preflight_stage'] = 'ACTIVE_TASK_SCAN'
        related_resources = list((await db.scalars(select(Resource).where(
            Resource.tmdb_id == tmdb_id,
            Resource.season == season,
        ))).all())
        related_by_id = {row.id: row for row in related_resources}
        related_tasks = list((await db.scalars(select(TransferQueueTask).where(
            TransferQueueTask.resource_id.in_(list(related_by_id))
        ))).all()) if related_by_id else []
        completed_keys: set[str] = set()
        active_keys: set[str] = set()
        payload['preflight_stage'] = 'SHARE_EPISODE_MAP'
        from app.transfer.episode_matcher import extract_video_episode_keys
        share_keys = {
            key
            for item in (share_listing.get('video_files') or [])
            for key in extract_video_episode_keys(str(item.get('name') or ''), known_season=season)
            if int(key[1:3]) == season
        }
        for episode_key in share_keys:
            targets = {episode_key}
            for other in related_tasks:
                if other.id == task.id:
                    continue
                if not task_matches_episode(
                    other.payload,
                    tmdb_id=tmdb_id,
                    season=season,
                    episode_keys=targets,
                    resource=related_by_id.get(other.resource_id),
                ):
                    continue
                status = str(other.status).upper()
                if status in {'SUCCESS', 'COMPLETED'}:
                    completed_keys.add(episode_key)
                elif _is_active_episode_task_status(status):
                    active_keys.add(episode_key)

        payload['preflight_stage'] = 'FINAL_DECISION'
        plan = plan_missing_episode_transfer(
            list(share_listing.get('video_files') or []),
            season=season,
            trigger_episode_keys=list(payload.get('episode_keys') or ([resource.episode_key] if resource.episode_key else [])),
            collected_episode_keys=list(watchlist.collected_episodes or []),
            inventory_episode_keys=inventory_keys,
            cloud_episode_keys=cloud_keys,
            completed_episode_keys=completed_keys,
            active_episode_keys=active_keys,
            cloud_scan_verified=cloud_scan_verified,
            cloud_scan_truncated=bool(cloud_scan and cloud_scan.get('truncated')),
            cloud_pagination_complete=cloud_pagination_complete,
        )
        return {
            **plan.as_dict(),
            '_share_evidence': share_listing,
            '_cloud_evidence': cloud_files,
            '_cloud_scan_verified': cloud_scan_verified,
            '_cloud_verified_episode_keys': sorted(cloud_keys),
            '_watchlist_id': watchlist.id,
            '_cloud_series_root_id': root_id,
            '_inventory_prefix': payload.get('inventory_prefix'),
            '_resource_title': resource.title,
        }

    async def _runtime_final_preflight(
        self,
        *,
        task_id: int,
        resource_id: int | None,
        payload: dict,
    ) -> dict | None:
        """Re-run all provider-facing gates after claim and before any restore.

        The persisted queue label is an optimization, never the security
        boundary.  This path rechecks share readability, exact selection,
        destination access and source rename planning immediately before the
        adapter can call a write endpoint.
        """
        if str(payload.get('operation') or '').strip().lower() == 'promote':
            return payload
        if str(payload.get('provider') or '').casefold() in {'dry-run', 'mock', 'test'}:
            # Isolated test/dry-run adapters have no provider API and cannot
            # represent production share or destination evidence. Real
            # providers never take this branch.
            return payload
        async with self.session_factory() as db, db.begin():
            task = await db.get(TransferQueueTask, task_id)
            resource = await db.get(Resource, resource_id) if resource_id is not None else None
            if task is None:
                logger.warning('Transfer task %s disappeared before final runtime preflight', task_id)
                return None
            if str(task.status) != str(TransferStatus.RUNNING) or str(task.locked_by or '') != self.worker_id:
                logger.warning('Transfer task %s is no longer owned by this worker before final runtime preflight', task_id)
                return None
            raw_episode_keys = payload.get('episode_keys') or ([resource.episode_key] if resource and resource.episode_key else [])
            canonical_batch_keys = {
                key
                for value in raw_episode_keys
                if (key := canonical_episode_key(int(resource.season or 1), value)) is not None
            } if resource is not None else set()
            if len(canonical_batch_keys) > 1:
                payload['selection_mode'] = 'MISSING_EPISODES'
            resume_stage = str(payload.get('execution_stage') or '').strip().upper()
            if resume_stage in {'RESTORED', 'RESTORE_VERIFIED', 'RENAMING', 'RENAME_VERIFIED'}:
                resume_ids = (
                    payload.get('selected_verified_file_ids')
                    or (payload.get('selection_snapshot') or {}).get('selected_file_ids')
                )
                resume_names = (
                    payload.get('selected_file_names')
                    or payload.get('selected_verified_names')
                    or payload.get('expected_files')
                )
                if not payload.get('remote_folder_id') or not resume_ids or not resume_names:
                    task.status = 'PENDING'
                    task.error_message = '[FINAL_PREFLIGHT:NEEDS_REVIEW] RESUME_FENCE_INCOMPLETE'
                    task.payload = {
                        **dict(task.payload or {}),
                        'preflight_classification': 'NEEDS_REVIEW',
                        'preflight_reason': 'RESUME_FENCE_INCOMPLETE',
                    }
                    task.locked_at = None
                    task.locked_by = None
                    await db.flush()
                    return None
                logger.info('Transfer task %s resumes from verified remote stage=%s without another restore', task_id, resume_stage)
                return payload
            batch_share_evidence = None
            cloud_verified = False
            cloud_verified_episode_keys: list[str] = []
            if str(payload.get('selection_mode') or '').upper() == 'MISSING_EPISODES':
                try:
                    async with asyncio.timeout(BATCH_PREFLIGHT_TIMEOUT_SECONDS):
                        batch_plan = await self._batch_episode_presence_preflight(
                            db,
                            task=task,
                            resource=resource,
                            payload=payload,
                        )
                        batch_share_evidence = batch_plan.pop('_share_evidence', None)
                        cloud_evidence = batch_plan.pop('_cloud_evidence', [])
                        cloud_verified = bool(batch_plan.pop('_cloud_scan_verified', False))
                        cloud_verified_episode_keys = batch_plan.pop('_cloud_verified_episode_keys', [])
                        watchlist_id = batch_plan.pop('_watchlist_id', None)
                        root_id = batch_plan.pop('_cloud_series_root_id', '')
                        inventory_prefix = batch_plan.pop('_inventory_prefix', None)
                        resource_title = batch_plan.pop('_resource_title', '')
                        metadata_reconcile = batch_plan.get('metadata_reconcile') or {}
                        if metadata_reconcile:
                            if resource is None:
                                raise RuntimeError('metadata reconciliation resource is missing')
                            if not cloud_verified or watchlist_id is None:
                                raise RuntimeError('metadata reconciliation lacks verified cloud evidence')
                            reconcile_stage = 'INVENTORY_READ' if any(
                                'inventory' in ledgers for ledgers in metadata_reconcile.values()
                            ) else 'COLLECTED_READ'
                            payload['preflight_stage'] = reconcile_stage
                            batch_plan['metadata_reconcile_result'] = await self._reconcile_verified_cloud_presence(
                                tmdb_id=int(resource.tmdb_id),
                                season=int(resource.season),
                                title=str(resource_title or resource.title or ''),
                                watchlist_id=int(watchlist_id),
                                series_root_id=str(root_id or ''),
                                inventory_prefix=inventory_prefix,
                                metadata_reconcile=metadata_reconcile,
                                cloud_files=cloud_evidence,
                                provider=str(payload.get('provider') or resource.cloud_name or 'guangya'),
                            )
                except TimeoutError as exc:
                    stage = str(payload.get('preflight_stage') or 'UNKNOWN')
                    detail = f'TimeoutError: BATCH_PREFLIGHT_TIMEOUT after {BATCH_PREFLIGHT_TIMEOUT_SECONDS:.0f}s at stage={stage}'
                    safe_exc = RuntimeError(detail)
                    logger.exception(
                        'Batch preflight failed task_id=%s resource_id=%s tmdb_id=%s season=%s stage=%s episode_count=%s detail=%s',
                        task_id, resource_id, getattr(resource, 'tmdb_id', None), getattr(resource, 'season', None),
                        stage, len(payload.get('episode_keys') or []), detail,
                        exc_info=(type(safe_exc), safe_exc, exc.__traceback__),
                    )
                    batch_plan = {
                        'classification': 'NEEDS_REVIEW',
                        'reason': 'BATCH_PREFLIGHT_TIMEOUT',
                        'detail': detail,
                        'preflight_stage': stage,
                    }
                    batch_share_evidence = None
                except Exception as exc:
                    stage = str(payload.get('preflight_stage') or 'UNKNOWN')
                    detail = _safe_preflight_detail(exc)
                    category = classify_error(exc)
                    reason = (
                        str(category)
                        if category in {
                            TransferErrorCategory.NETWORK_TIMEOUT,
                            TransferErrorCategory.NETWORK_ERROR,
                            TransferErrorCategory.REMOTE_5XX,
                            TransferErrorCategory.RATE_LIMITED,
                        }
                        else 'BATCH_PREFLIGHT_EXCEPTION'
                    )
                    safe_exc = RuntimeError(detail)
                    logger.exception(
                        'Batch preflight failed task_id=%s resource_id=%s tmdb_id=%s season=%s stage=%s episode_count=%s exception_type=%s detail=%s',
                        task_id, resource_id, getattr(resource, 'tmdb_id', None), getattr(resource, 'season', None),
                        stage, len(payload.get('episode_keys') or []), type(exc).__name__, detail,
                        exc_info=(type(safe_exc), safe_exc, exc.__traceback__),
                    )
                    batch_plan = {
                        'classification': 'NEEDS_REVIEW',
                        'reason': reason,
                        'detail': detail,
                        'preflight_stage': stage,
                    }
                    batch_share_evidence = None
                batch_classification = str(batch_plan.get('classification') or 'NEEDS_REVIEW')
                batch_reason = str(batch_plan.get('reason') or 'BATCH_PREFLIGHT_INCOMPLETE')
                if batch_reason == 'NO_MISSING_EPISODES' and cloud_verified and resource is not None:
                    batch_plan['stale_pending_recovery'] = await self._recover_stale_pending_after_final_preflight(
                        db,
                        current_task=task,
                        resource=resource,
                        payload={
                            **payload,
                            'batch_presence_preflight': {
                                'cloud_scan_verified': True,
                                'presence_decisions': batch_plan.get('presence_decisions') or {},
                            },
                        },
                        missing_episode_keys=[],
                    )
                batch_stage = str(batch_plan.get('preflight_stage') or payload.get('preflight_stage') or 'UNKNOWN')
                batch_detail = batch_plan.get('detail') or batch_plan.get('error') or ''
                if is_retryable_provider_failure(batch_reason, detail=batch_detail, stage=batch_stage):
                    await self._defer_provider_network_failure(
                        db,
                        task=task,
                        payload=payload,
                        reason=batch_reason,
                        stage=batch_stage,
                        detail=batch_detail,
                    )
                    return None
                if batch_classification != AUTO_SAFE:
                    task.status = REVIEW_STATUS
                    if batch_reason == 'NO_MISSING_EPISODES':
                        batch_reason = 'RECONCILED_ALREADY_IN_CLOUD'
                    task.error_message = f'[FINAL_PREFLIGHT:{batch_classification}] stage={batch_stage} {batch_reason}'[:4000]
                    task.payload = {
                        **dict(task.payload or {}),
                        'preflight_classification': batch_classification,
                        'preflight_reason': batch_reason,
                        'preflight_stage': batch_stage,
                        'batch_presence_preflight': batch_plan,
                    }
                    task.locked_at = None
                    task.locked_by = None
                    await db.flush()
                    logger.warning('Transfer task %s blocked by batch presence preflight: stage=%s %s %s', task_id, batch_stage, batch_classification, batch_reason)
                    return None
                missing_keys = [str(value) for value in batch_plan.get('missing_episode_keys') or []]
                if not missing_keys:
                    task.status = REVIEW_STATUS
                    task.error_message = '[FINAL_PREFLIGHT:REJECTED] RECONCILED_ALREADY_IN_CLOUD'
                    task.payload = {
                        **dict(task.payload or {}),
                        'preflight_classification': 'REJECTED',
                        'preflight_reason': 'RECONCILED_ALREADY_IN_CLOUD',
                        'preflight_stage': batch_stage,
                        'batch_presence_preflight': batch_plan,
                    }
                    task.locked_at = None
                    task.locked_by = None
                    await db.flush()
                    return None
                payload['episode_keys'] = missing_keys
                payload['selected_episode_keys'] = missing_keys
                payload['selection_mode'] = 'MISSING_EPISODES'
                payload['batch_presence_preflight'] = {
                    'classification': batch_classification,
                    'reason': batch_reason,
                    'preflight_stage': batch_stage,
                    'share_episode_keys': list(batch_plan.get('share_episode_keys') or []),
                    'missing_episode_keys': missing_keys,
                    'presence_decisions': batch_plan.get('presence_decisions') or {},
                    'metadata_reconcile': batch_plan.get('metadata_reconcile') or {},
                    'metadata_reconcile_result': batch_plan.get('metadata_reconcile_result') or {},
                    'cloud_scan_verified': cloud_verified,
                    'cloud_verified_episode_keys': list(cloud_verified_episode_keys),
                }
                for stale_key in ('selection_snapshot', 'selected_file_ids', 'selected_file_names', 'selected_episode_by_file_id'):
                    payload.pop(stale_key, None)
            route = {
                'media_root': payload.get('media_root'),
                'media_category': payload.get('media_category'),
                'inventory_prefix': payload.get('inventory_prefix'),
            }
            try:
                async def remote_validator(**kwargs):
                    target = str(payload.get('target_folder_id') or kwargs.get('target_folder_id') or '')
                    validator_kwargs = {**kwargs, 'target_folder_id': target}
                    if batch_share_evidence is not None:
                        validator_kwargs['share_override'] = batch_share_evidence
                    return await validate_remote_canary(**validator_kwargs)

                report = await preflight_task(
                    db,
                    task_id,
                    remote_validator=remote_validator,
                    allow_running_locked_by=self.worker_id,
                    episode_keys_override=list(payload.get('episode_keys') or []),
                    selection_mode_override=str(payload.get('selection_mode') or '') or None,
                )
                if resource is None:
                    decision = {
                        'classification': 'REJECTED',
                        'reason': 'RESOURCE_MISSING',
                        'detail': f'resource_id={resource_id}',
                    }
                    rename_plan = {'status': 'NOT_CHECKED', 'reason': 'RESOURCE_MISSING'}
                else:
                    rename_plan = build_source_rename_plan(report, payload=payload, resource=resource)
                    decision = classify_final_preflight(
                        report,
                        route=route,
                        rename_plan=rename_plan,
                    )
            except Exception as exc:  # noqa: BLE001 - runtime preflight must fail closed
                decision = {
                    'classification': 'NEEDS_REVIEW',
                    'reason': 'RUNTIME_PREFLIGHT_EXCEPTION',
                    'detail': f'{type(exc).__name__}: {str(exc)[:300]}',
                }
                rename_plan = {'status': 'NOT_CHECKED', 'reason': decision['reason']}
                report = {}

            classification = str(decision['classification'])
            reason = str(decision['reason'])
            decision_stage = str(payload.get('preflight_stage') or 'SHARE_PROBE')
            if classification != AUTO_SAFE and is_retryable_provider_failure(
                reason, detail=decision.get('detail') or '', stage=decision_stage
            ):
                await self._defer_provider_network_failure(
                    db,
                    task=task,
                    payload=payload,
                    reason=reason,
                    stage=decision_stage,
                    detail=decision.get('detail') or '',
                )
                return None
            if classification != AUTO_SAFE:
                task.status = 'PENDING'
                task.error_message = f'[FINAL_PREFLIGHT:{classification}] {reason}'[:4000]
                task.payload = {
                    **dict(task.payload or {}),
                    'preflight_classification': classification,
                    'preflight_reason': reason,
                    'preflight_runtime_verified_at': datetime.now(UTC).isoformat(),
                    'preflight_rename_status': rename_plan.get('status'),
                }
                task.locked_at = None
                task.locked_by = None
                await db.flush()
                logger.warning('Transfer task %s blocked by runtime final preflight: %s %s', task.id, classification, reason)
                return None

            remote = report.get('remote_validation') or {}
            selected_ids = [str(value).strip() for value in (remote.get('selected_file_ids') or []) if str(value).strip()]
            selected_names = [str(value).strip() for value in (remote.get('selected_file_names') or []) if str(value).strip()]
            selection_mode = str(remote.get('selection_mode') or '').strip()
            selected_episode_keys = [str(value).strip() for value in (remote.get('selected_episode_keys') or []) if str(value).strip()]
            episode_file_map = {
                str(key).strip(): str(value).strip()
                for key, value in (remote.get('episode_file_map') or {}).items()
                if str(key).strip() and str(value).strip()
            }
            if (
                selection_mode == 'MISSING_EPISODES'
                and (
                    set(episode_file_map) != set(payload.get('episode_keys') or [])
                    or set(episode_file_map.values()) != set(selected_ids)
                    or set(selected_episode_keys) != set(payload.get('episode_keys') or [])
                )
            ):
                task.status = 'PENDING'
                task.error_message = '[FINAL_PREFLIGHT:NEEDS_REVIEW] RUNTIME_EPISODE_SELECTION_MAP_INCOMPLETE'
                task.payload = {
                    **dict(task.payload or {}),
                    'preflight_classification': 'NEEDS_REVIEW',
                    'preflight_reason': 'RUNTIME_EPISODE_SELECTION_MAP_INCOMPLETE',
                }
                task.locked_at = None
                task.locked_by = None
                await db.flush()
                return None
            if not selected_ids or not selected_names or not selection_mode:
                task.status = 'PENDING'
                task.error_message = '[FINAL_PREFLIGHT:NEEDS_REVIEW] RUNTIME_SELECTION_SNAPSHOT_INCOMPLETE'
                task.payload = {
                    **dict(task.payload or {}),
                    'preflight_classification': 'NEEDS_REVIEW',
                    'preflight_reason': 'RUNTIME_SELECTION_SNAPSHOT_INCOMPLETE',
                    'preflight_runtime_verified_at': datetime.now(UTC).isoformat(),
                }
                task.locked_at = None
                task.locked_by = None
                await db.flush()
                logger.warning('Transfer task %s blocked: runtime selection snapshot incomplete', task.id)
                return None
            recovery = await self._recover_stale_pending_after_final_preflight(
                db,
                current_task=task,
                resource=resource,
                payload=payload,
                missing_episode_keys=list(payload.get('episode_keys') or []),
            ) if resource is not None and selection_mode == 'MISSING_EPISODES' else {
                'queued_reactivated': 0,
                'stale_pending_recovered': 0,
                'reconciled': 0,
            }
            payload['queued_reactivated'] = bool(recovery.get('queued_reactivated'))
            payload['stale_pending_recovered'] = bool(recovery.get('stale_pending_recovered'))
            payload['candidate_switched'] = bool(recovery.get('queued_reactivated'))
            if recovery.get('queued_reactivated'):
                logger.info(
                    'stale PENDING task reactivated after verified AUTO_SAFE preflight: old_task=%s replacement_task=%s',
                    recovery.get('reactivated_task_id'),
                    task.id,
                )
                return None
            snapshot = {
                'selection_mode': selection_mode,
                'selected_file_ids': selected_ids,
                'selected_file_names': selected_names,
                'selected_episode_keys': selected_episode_keys,
                'episode_file_map': episode_file_map,
            }
            if episode_file_map:
                payload['selected_episode_keys'] = selected_episode_keys
                payload['selected_episode_by_file_id'] = {
                    file_id: episode_key for episode_key, file_id in episode_file_map.items()
                }
                payload['episode_keys'] = selected_episode_keys
            payload.update({
                'preflight_classification': AUTO_SAFE,
                'preflight_reason': reason,
                'preflight_runtime_verified_at': datetime.now(UTC).isoformat(),
                'preflight_rename_status': rename_plan.get('status'),
                'selection_snapshot': snapshot,
            })
            task.payload = {
                **dict(task.payload or {}),
                'selection_mode': selection_mode,
                'episode_keys': list(payload.get('episode_keys') or []),
                'selected_episode_keys': list(payload.get('selected_episode_keys') or []),
                'selected_episode_by_file_id': dict(payload.get('selected_episode_by_file_id') or {}),
                'batch_presence_preflight': payload.get('batch_presence_preflight'),
                'preflight_classification': AUTO_SAFE,
                'preflight_reason': reason,
                'preflight_runtime_verified_at': payload['preflight_runtime_verified_at'],
                'preflight_rename_status': rename_plan.get('status'),
                'selection_snapshot': snapshot,
            }
            await db.flush()
            logger.info('Transfer task %s passed runtime final preflight: exact selection=%s', task.id, selected_ids)
            return payload

    # ------------------------------------------------------------------ #
    # Transaction A: claim
    # ------------------------------------------------------------------ #

    async def _claim_payload(self) -> tuple[int, int | None, dict] | None:
        """Claim one task and hydrate its payload; returns (task_id, resource_id, payload)."""
        async with self.session_factory() as db:
            if await BotSettingsService.is_transfer_paused(db):
                logger.info('Transfer queue consumption skipped: transfer pause is enabled')
                return None
        if not await self._provider_claim_allowed():
            logger.info('Transfer queue consumption deferred by Guangya provider circuit')
            return None
        async with self.session_factory() as db, db.begin():
            if await BotSettingsService.is_transfer_paused(db):
                logger.info('Transfer queue consumption skipped: transfer pause is enabled')
                return None
            task = await TransferQueueService.claim_next(db, worker_id=self.worker_id)
            if not task:
                return None
            payload = {}
            try:
                payload = await self._hydrate_payload(db, task)
                resource = await db.get(Resource, task.resource_id) if task.resource_id is not None else None
                if not payload.get('selection_mode') and resource is not None and resource.season:
                    canonical_keys = {
                        key
                        for value in (payload.get('episode_keys') or ([resource.episode_key] if resource.episode_key else []))
                        if (key := canonical_episode_key(int(resource.season), value)) is not None
                    }
                    if len(canonical_keys) > 1:
                        payload['selection_mode'] = 'MISSING_EPISODES'
                gate = await self._final_local_preflight(db, task, payload)
            except DestinationMetadataIncomplete as exc:
                gate = ('NEEDS_REVIEW', str(exc))
            if gate is not None:
                classification, reason = gate
                task.status = 'PENDING'
                task.error_message = f'[FINAL_PREFLIGHT:{classification}] {reason}'[:4000]
                task.payload = {
                    **dict(task.payload or {}),
                    **payload,
                    'preflight_classification': classification,
                    'preflight_reason': reason,
                }
                task.locked_at = None
                task.locked_by = None
                await db.flush()
                logger.warning('Transfer task %s blocked by final preflight: %s %s', task.id, classification, reason)
                return None
            return task.id, task.resource_id, payload

    async def _notify_success(
        self,
        *,
        task_id: int,
        task_payload: dict,
        transfer_result: dict,
        resource: Resource | None,
    ) -> NotificationResult:
        if self.notifier is None:
            return NotificationResult('NOTIFICATION_TARGET_MISSING', False, error='notifier disabled')
        try:
            method = getattr(self.notifier, 'notify_success_result', None)
            if method is not None:
                result = await method(
                    task_payload={**task_payload, 'task_id': task_id},
                    transfer_result=transfer_result,
                    resource=resource,
                )
            else:
                sent = await self.notifier.notify_success(
                    task_payload=task_payload,
                    transfer_result=transfer_result,
                    resource=resource,
                )
                result = NotificationResult('SENT' if sent else 'NOTIFICATION_FAILED', bool(sent))
            logger.info(
                'transfer notification result task=%s status=%s target_source=%s',
                task_id,
                result.status,
                result.target_source or 'none',
            )
            return result
        except Exception as notif_err:  # noqa: BLE001 - notification never changes transfer state
            logger.warning('Failed to send success notification for task %s: %s', task_id, notif_err)
            return NotificationResult('NOTIFICATION_FAILED', False, error=type(notif_err).__name__)

    async def _notify_failure(
        self,
        *,
        task_id: int,
        task_payload: dict,
        error_message: str,
        resource: Resource | None,
        attempts: int,
        category: str | None,
    ) -> NotificationResult:
        if self.notifier is None:
            return NotificationResult('NOTIFICATION_TARGET_MISSING', False, error='notifier disabled')
        try:
            method = getattr(self.notifier, 'notify_failure_result', None)
            if method is not None:
                result = await method(
                    task_payload=task_payload,
                    error_message=error_message,
                    resource=resource,
                    attempts=attempts,
                    task_id=task_id,
                    category=category,
                    stage='unknown',
                )
            else:
                sent = await self.notifier.notify_failure(
                    task_payload=task_payload,
                    error_message=error_message,
                    resource=resource,
                    attempts=attempts,
                    task_id=task_id,
                    category=category,
                    stage='unknown',
                )
                result = NotificationResult('SENT' if sent else 'NOTIFICATION_FAILED', bool(sent))
            logger.info(
                'transfer notification result task=%s status=%s target_source=%s',
                task_id,
                result.status,
                result.target_source or 'none',
            )
            return result
        except Exception as notif_err:  # noqa: BLE001 - notification never changes transfer state
            logger.warning('Failed to send failure notification for task %s: %s', task_id, notif_err)
            return NotificationResult('NOTIFICATION_FAILED', False, error=type(notif_err).__name__)

    async def _persist_notification_result(self, task_id: int, result: NotificationResult) -> None:
        """Record notification outcome without touching the transfer terminal state."""
        try:
            async with self.session_factory() as db, db.begin():
                task = await db.get(TransferQueueTask, task_id)
                if task is None:
                    return
                task.result = {**dict(task.result or {}), 'notification': result.as_dict()}
        except Exception as exc:  # noqa: BLE001 - observability failure is not transfer failure
            logger.warning('Could not persist notification result for task %s: %s', task_id, exc)

    async def _prepare_success_notification_payload(
        self,
        *,
        task_payload: dict,
        resource_id: int | None,
        transfer_result: dict,
    ) -> dict:
        """Re-read completed ledgers before composing cumulative progress."""
        notification_payload = dict(task_payload)
        if resource_id is None:
            notification_payload['collection_progress_verified'] = False
            return notification_payload
        async with self.session_factory() as db:
            resource = await db.get(Resource, resource_id)
            if resource is None or not resource.tmdb_id or not resource.season:
                notification_payload['collection_progress_verified'] = False
                return notification_payload
            season = int(resource.season)
            watchlists = list((await db.scalars(
                select(SeriesWatchlist).where(
                    SeriesWatchlist.tmdb_id == int(resource.tmdb_id),
                    SeriesWatchlist.season == season,
                    SeriesWatchlist.status != 'CANCELLED',
                ).order_by(SeriesWatchlist.id.asc())
            )).all())
            if len(watchlists) != 1:
                notification_payload['collection_progress_verified'] = False
                return notification_payload
            watchlist = watchlists[0]
            inventory_rows = list((await db.scalars(
                select(CloudDiskInventory).where(
                    CloudDiskInventory.tmdb_id == int(resource.tmdb_id),
                    CloudDiskInventory.season == season,
                )
            )).all())
        collected_keys = {
            key
            for value in (watchlist.collected_episodes or [])
            if (key := canonical_episode_key(season, value)) is not None
        }
        inventory_keys = {
            f'S{season:02d}E{int(row.episode):02d}'
            for row in inventory_rows
            if row.episode is not None
        }
        batch_evidence = notification_payload.get('batch_presence_preflight') or {}
        cloud_scan_verified = bool(batch_evidence.get('cloud_scan_verified'))
        cloud_keys = {
            key
            for value in batch_evidence.get('cloud_verified_episode_keys') or []
            if (key := canonical_episode_key(season, value)) is not None
        }
        if transfer_result.get('verified') and transfer_result.get('inventory', {}).get('status') == 'SYNCED':
            cloud_keys.update(
                key
                for value in transfer_result.get('selected_episode_keys') or []
                if (key := canonical_episode_key(season, value)) is not None
            )
        progress_verified = (
            cloud_scan_verified
            and collected_keys == inventory_keys
            and collected_keys == cloud_keys
        )
        notification_payload.update({
            'season': season,
            'total_episodes': int(watchlist.total_episodes or notification_payload.get('total_episodes') or 0),
            'collected_episodes': sorted(collected_keys),
            'collected_episode_keys': sorted(collected_keys),
            'inventory_episode_keys': sorted(inventory_keys),
            'cloud_verified_episode_keys': sorted(cloud_keys),
            'inventory_count': len(inventory_keys),
            'cloud_count': len(cloud_keys),
            'collection_progress_verified': progress_verified,
        })
        total = int(notification_payload['total_episodes'] or 0)
        if total > 0:
            expected = {f'S{season:02d}E{number:02d}' for number in range(1, total + 1)}
            notification_payload['missing_episode_keys'] = sorted(expected - collected_keys)
        else:
            notification_payload['missing_episode_keys'] = []
        return notification_payload

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
            all_verified_files = list(dict.fromkeys(
                str(name).strip()
                for name in (outcome.remote_files or ())
                if str(name).strip()
            )) if outcome.verified else []
            selected_names = {
                str(name).strip()
                for name in (payload.get('selected_file_names') or payload.get('expected_files') or [])
                if str(name).strip()
            }
            verified_files = [
                name for name in all_verified_files
                if not selected_names or name in selected_names
            ]
            transfer_result = {
                'verified': outcome.verified,
                'remote_folder_id': outcome.remote_folder_id,
                'remote_files': all_verified_files,
                'selected_file_names': verified_files,
                'selected_file_sizes': dict(payload.get('selected_file_sizes') or {}),
                'remote_file_records': list(outcome.remote_file_records or ()),
                'rename_status': outcome.rename_status or payload.get('rename_status'),
                'destination_kind': payload.get('destination_kind'),
            }
            verified_episode_files = list(outcome.verified_episode_files or payload.get('verified_episode_files') or [])
            verified_episode_by_name: dict[str, dict] = {}
            episode_integrity_error = None
            if not is_promotion and resource is not None and resource.season is not None:
                raw_expected_keys = (
                    payload.get('selected_episode_keys')
                    or payload.get('episode_keys')
                    or ([resource.episode_key] if resource.episode_key else [])
                )
                expected_episode_keys = {
                    key for value in raw_expected_keys
                    if (key := canonical_episode_key(resource.season, value)) is not None
                }
                for item in verified_episode_files:
                    name = str(item.get('file_name') or item.get('name') or '').strip()
                    key = canonical_episode_key(resource.season, item.get('episode_key'))
                    if not name or not key or name in verified_episode_by_name or key in {
                        str(row.get('episode_key')) for row in verified_episode_by_name.values()
                    }:
                        episode_integrity_error = 'VERIFIED_EPISODE_MAP_INVALID'
                        break
                    verified_episode_by_name[name] = {**dict(item), 'file_name': name, 'episode_key': key}
                if not verified_episode_by_name and len(verified_files) == 1 and len(expected_episode_keys) == 1:
                    name = verified_files[0]
                    remote_record = next(
                        (row for row in outcome.remote_file_records or () if str(row.get('name') or row.get('file_name') or '').strip() == name),
                        {},
                    )
                    key = next(iter(expected_episode_keys))
                    verified_episode_by_name[name] = {
                        'episode_key': key,
                        'file_name': name,
                        'file_id': str(remote_record.get('file_id') or remote_record.get('fileId') or ''),
                        'size': remote_record.get('size') or remote_record.get('fileSize') or 0,
                    }
                observed_episode_keys = {str(row['episode_key']) for row in verified_episode_by_name.values()}
                if episode_integrity_error is None and (
                    set(verified_episode_by_name) != set(verified_files)
                    or not expected_episode_keys
                    or observed_episode_keys != expected_episode_keys
                ):
                    episode_integrity_error = 'VERIFIED_EPISODE_MAP_INCOMPLETE'
                if episode_integrity_error:
                    await BotSettingsService.set_transfer_paused(db, True)
                    task.status = TransferStatus.RETRY_WAIT
                    task.next_run_at = datetime.now(UTC)
                    task.error_message = f'[SYSTEM_PAUSE:{episode_integrity_error}] verified readback closure is incomplete'[:4000]
                    task.payload = {
                        **dict(task.payload or {}),
                        **payload,
                        'execution_stage': 'RENAME_VERIFIED',
                        'remote_folder_id': outcome.remote_folder_id,
                        'selected_file_names': verified_files,
                        'verified_remote_records': list(outcome.remote_file_records or ()),
                        'preflight_classification': 'NEEDS_REVIEW',
                        'preflight_reason': episode_integrity_error,
                    }
                    task.result = {
                        **dict(task.result or {}),
                        'verified': bool(outcome.verified),
                        'selected_file_names': verified_files,
                        'integrity_error': episode_integrity_error,
                    }
                    task.locked_at = None
                    task.locked_by = None
                    await db.flush()
                    logger.critical('transfer_paused=1: task=%s %s', task_id, episode_integrity_error)
                    return False
                transfer_result['verified_episode_files'] = list(verified_episode_by_name.values())
                transfer_result['selected_episode_keys'] = sorted(observed_episode_keys)
            inventory_result: dict = {
                'status': 'NOT_ATTEMPTED',
                'written': 0,
                'results': [],
            }
            if is_promotion:
                if not outcome.verified or resource is None or resource.tmdb_id is None:
                    inventory_result.update({
                        'status': 'PROMOTION_UNVERIFIED',
                        'reason': 'PROMOTION_READBACK_NOT_VERIFIED',
                    })
                else:
                    promotion_inventory = await CloudInventoryService.update_rel_paths_after_promotion(
                        db,
                        tmdb_id=resource.tmdb_id,
                        series_folder_name=str(payload.get('series_folder_name') or resource.title or resource.tmdb_id),
                        destination_prefix=payload.get('destination_prefix'),
                        relevant_seasons=payload.get('relevant_seasons'),
                    )
                    inventory_result.update({
                        'status': 'PROMOTION_PATHS_UPDATED',
                        'written': promotion_inventory.get('changed', 0),
                        'results': [promotion_inventory],
                    })
                    transfer_result['promotion_status'] = outcome.promotion_status or payload.get('promotion_status') or 'PROMOTION_COMPLETED'
            elif not outcome.verified:
                inventory_result.update({
                    'status': 'SKIPPED_UNVERIFIED',
                    'reason': 'READBACK_NOT_VERIFIED',
                })
            elif resource is None or resource.tmdb_id is None or resource.season is None:
                inventory_result.update({
                    'status': 'INVENTORY_SYNC_FAILED',
                    'error': 'RESOURCE_IDENTITY_MISSING',
                })
            else:
                selected_names = {
                    str(name).strip()
                    for name in (payload.get('selected_file_names') or payload.get('expected_files') or [])
                    if str(name).strip()
                }
                inventory_files = [
                    name for name in verified_files
                    if not selected_names or name in selected_names
                ]
                sync_errors: list[str] = []
                for file_name in inventory_files:
                    episode_record = verified_episode_by_name.get(file_name)
                    if episode_record is None:
                        sync_errors.append('VERIFIED_EPISODE_MAP_MISSING')
                        continue
                    try:
                        async with db.begin_nested():
                            sync = await CloudInventoryService.upsert_verified_transfer(
                                db,
                                tmdb_id=resource.tmdb_id,
                                title=resource.title or payload.get('title'),
                                season=resource.season,
                                episode_key=str(episode_record['episode_key']),
                                file_name=file_name,
                                verified=True,
                                remote_file_id=str(episode_record.get('file_id') or '') or None,
                                remote_folder_id=outcome.remote_folder_id,
                                provider=payload.get('provider') or resource.cloud_name,
                                source=resource.source_type,
                                rel_path=(
                                    f"{str(payload.get('remote_rel_path_prefix')).strip('/')}/{file_name}"
                                    if payload.get('remote_rel_path_prefix')
                                    else payload.get('remote_rel_path')
                                ),
                                verified_at=datetime.now(UTC),
                            )
                        inventory_result['results'].append(sync.as_dict())
                        if sync.persisted:
                            inventory_result['written'] += 1
                        else:
                            sync_errors.append(str(sync.reason or sync.status or 'INVENTORY_NOT_PERSISTED'))
                    except Exception as inventory_exc:
                        sync_errors.append(type(inventory_exc).__name__)
                        logger.exception(
                            'Inventory sync failed after verified transfer task=%s file=%s',
                            task_id,
                            file_name,
                        )
                if sync_errors:
                    inventory_result.update({
                        'status': 'INVENTORY_SYNC_FAILED',
                        'errors': sync_errors,
                    })
                elif inventory_result['results']:
                    inventory_result['status'] = 'SYNCED'
                else:
                    inventory_result.update({
                        'status': 'SKIPPED_NO_VERIFIED_SELECTED_FILE',
                        'reason': 'NO_SELECTED_READBACK_FILE',
                    })
            transfer_result['inventory'] = inventory_result
            if resource is not None and verified_files and not is_promotion:
                # Retain earlier files for this same resource only when Inventory
                # already proves their identity, then append the current readback set.
                preserved_files: list[str] = []
                if resource.tmdb_id and resource.season:
                    known_inventory_names = set((await db.scalars(select(CloudDiskInventory.file_name).where(
                        CloudDiskInventory.tmdb_id == resource.tmdb_id,
                        CloudDiskInventory.season == resource.season,
                    ))).all())
                    preserved_files = [
                        str(value).strip()
                        for value in (resource.file_names or [])
                        if str(value).strip() in known_inventory_names
                    ]
                resource.file_names = list(dict.fromkeys([*preserved_files, *verified_files]))
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
            task.payload = {**dict(task.payload or {}), **payload}
            if (
                not is_promotion
                and resource is not None
                and resource.season is not None
                and inventory_result.get('status') != 'SYNCED'
            ):
                await BotSettingsService.set_transfer_paused(db, True)
                task.status = TransferStatus.RETRY_WAIT
                task.next_run_at = datetime.now(UTC)
                task.error_message = '[SYSTEM_PAUSE:INVENTORY_SYNC_FAILED] verified files are not fully inventoried'[:4000]
                task.payload = {
                    **dict(task.payload or {}),
                    'execution_stage': 'RENAME_VERIFIED',
                    'remote_folder_id': outcome.remote_folder_id,
                    'selected_file_names': verified_files,
                    'verified_remote_records': list(outcome.remote_file_records or ()),
                }
                task.result = {
                    **dict(task.result or {}),
                    **transfer_result,
                    'integrity_error': 'INVENTORY_SYNC_FAILED',
                }
                task.locked_at = None
                task.locked_by = None
                await db.flush()
                logger.critical('transfer_paused=1: task=%s Inventory closure failed', task_id)
                return False
            verified_episode_keys = list(dict.fromkeys(
                str(record['episode_key'])
                for record in verified_episode_by_name.values()
                if record.get('episode_key')
            ))
            if not verified_episode_keys:
                verified_episode_keys = list(payload.get('episode_keys') or ([resource.episode_key] if resource and resource.episode_key else []))
            if not is_promotion and resource is not None and resource.tmdb_id and resource.season and verified_episode_keys:
                try:
                    async with db.begin_nested():
                        await WatchlistService.mark_collected(
                            db,
                            tmdb_id=resource.tmdb_id,
                            season=resource.season,
                            episode_keys=verified_episode_keys,
                        )
                except Exception as collected_exc:  # noqa: BLE001 - pause and retain verified remote fence on DB closure failure
                    await BotSettingsService.set_transfer_paused(db, True)
                    task.status = TransferStatus.RETRY_WAIT
                    task.next_run_at = datetime.now(UTC)
                    task.error_message = '[SYSTEM_PAUSE:COLLECTED_SYNC_FAILED] verified files are not fully collected'[:4000]
                    task.payload = {
                        **dict(task.payload or {}),
                        **payload,
                        'execution_stage': 'RENAME_VERIFIED',
                        'remote_folder_id': outcome.remote_folder_id,
                        'selected_file_names': verified_files,
                        'verified_remote_records': list(outcome.remote_file_records or ()),
                    }
                    task.result = {
                        **dict(task.result or {}),
                        **transfer_result,
                        'integrity_error': 'COLLECTED_SYNC_FAILED',
                    }
                    task.locked_at = None
                    task.locked_by = None
                    await db.flush()
                    logger.critical(
                        'transfer_paused=1: task=%s collected update failed (%s)',
                        task_id,
                        type(collected_exc).__name__,
                    )
                    return False
            if resource is not None and not is_promotion:
                resource.status = ResourceStatus.COMPLETED
            if resource is not None and not is_promotion and resource.tmdb_id and resource.season:
                from app.transfer.candidate_service import (
                    get_candidates_for_episode,
                    mark_candidate_transferred,
                )

                try:
                    async with db.begin_nested():
                        for episode_key in verified_episode_keys:
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
                except Exception as candidate_exc:  # noqa: BLE001 - best-effort ancillary candidate ledger
                    logger.warning('Candidate ledger update failed for task %s: %s', task_id, type(candidate_exc).__name__)
            await TransferQueueService.mark_completed(db, task, transfer_result)
        notification_payload = (
            payload
            if is_promotion
            else await self._prepare_success_notification_payload(
                task_payload=payload,
                resource_id=resource_id,
                transfer_result=transfer_result,
            )
        )
        notification_result = (
            NotificationResult('NOTIFICATION_SKIPPED_PROMOTION_PREVIEW', False, error='PROMOTION_CARD_PREVIEW_ONLY')
            if is_promotion
            else await self._notify_success(
                task_id=task_id,
                task_payload=notification_payload,
                transfer_result=transfer_result,
                resource=resource,
            )
        )
        await self._persist_notification_result(task_id, notification_result)
        return True

    # ------------------------------------------------------------------ #
    # Transaction C: classified failure
    # ------------------------------------------------------------------ #

    async def _record_failure(self, task_id: int, resource_id: int | None, payload: dict, exc: BaseException) -> bool:
        category = classify_error(exc)
        message = str(exc) or exc.__class__.__name__
        rename_resume = isinstance(exc, RenameUnverifiedError)
        promotion_operation = str(payload.get('operation') or '').strip().casefold() == 'promote'
        promotion_resume = False
        exception_code = str(getattr(exc, 'code', '') or '')
        scope_integrity_failure = (
            category == TransferErrorCategory.TRANSFER_SCOPE_VIOLATION
            or exception_code == 'DESTINATION_EPISODE_OVERLAP'
        )
        async with self.session_factory() as db, db.begin():
            task = await db.get(TransferQueueTask, task_id)
            if task is None:
                logger.warning('transfer task %s disappeared before failure recording', task_id)
                return True
            resource = await db.get(Resource, resource_id) if resource_id is not None else None
            persisted_payload = dict(task.payload or {})
            stage = str(payload.get('promotion_stage') or persisted_payload.get('promotion_stage') or '').upper()
            promotion_fenced = stage in {'MOVE_SUBMITTED', 'MOVED'}
            promotion_resume = promotion_operation and promotion_fenced
            if promotion_operation and isinstance(exc, PromotionUnverifiedError) and not promotion_fenced:
                task.status = 'PENDING'
                task.error_message = f'[PROMOTION_NEEDS_REVIEW] {message}'[:4000]
                task.payload = {
                    **persisted_payload,
                    **payload,
                    'promotion_status': 'NEEDS_REVIEW',
                    'preflight_classification': 'NEEDS_REVIEW',
                    'preflight_reason': 'PROMOTION_ROOT_OR_SOURCE_NOT_VERIFIED',
                }
                task.locked_at = None
                task.locked_by = None
                await db.flush()
                return False
            if category == TransferErrorCategory.FILE_SELECTION_REVIEW:
                review_code = str(getattr(exc, 'code', '') or category)
                task.status = 'PENDING'
                task.error_message = f'[FINAL_PREFLIGHT:NEEDS_REVIEW] {review_code} {message}'[:4000]
                task.payload = {
                    **persisted_payload,
                    **payload,
                    'preflight_classification': 'NEEDS_REVIEW',
                    'preflight_reason': review_code,
                    'structural_conflict': True,
                }
                task.locked_at = None
                task.locked_by = None
                await db.flush()
                return False
            if (
                str(payload.get('provider') or getattr(resource, 'cloud_name', '') or '').casefold() == 'guangya'
                and category in {
                    TransferErrorCategory.NETWORK_TIMEOUT,
                    TransferErrorCategory.NETWORK_ERROR,
                    TransferErrorCategory.REMOTE_5XX,
                    TransferErrorCategory.RATE_LIMITED,
                }
            ):
                await record_provider_network_failure(db, task_id=int(task.id))
            if scope_integrity_failure:
                await BotSettingsService.set_transfer_paused(db, True)
                task.status = 'PENDING'
                task.error_message = f'[SYSTEM_PAUSE:{exception_code or category}] {message}'[:4000]
                task.payload = {
                    **dict(task.payload or {}),
                    **payload,
                    'preflight_classification': 'NEEDS_REVIEW',
                    'preflight_reason': exception_code or str(category),
                    'scope_integrity_hold': True,
                    'scope_integrity_code': exception_code or str(category),
                }
                task.result = {
                    **dict(task.result or {}),
                    'integrity_error': exception_code or str(category),
                }
                task.locked_at = None
                task.locked_by = None
                await db.flush()
                logger.critical('transfer_paused=1: task=%s scope integrity failure=%s', task_id, exception_code or category)
                return False
            if rename_resume or promotion_resume:
                resume_exc = exc
                if promotion_resume:
                    task.payload = {
                        **dict(task.payload or {}),
                        **payload,
                        'promotion_stage': stage,
                        'promotion_series_folder_id': getattr(resume_exc, 'series_folder_id', None) or payload.get('promotion_series_folder_id'),
                        'promotion_status': 'PROMOTION_UNVERIFIED',
                        'promotion_readback': {
                            'status': 'PROMOTION_UNVERIFIED',
                            'error': message[:1000],
                        },
                    }
                else:
                    task.payload = {
                        **dict(task.payload or {}),
                        **payload,
                        'execution_stage': 'RENAMING',
                        'remote_folder_id': getattr(resume_exc, 'remote_folder_id', None) or payload.get('remote_folder_id'),
                        'verified_remote_records': getattr(resume_exc, 'remote_records', None) or payload.get('verified_remote_records') or [],
                    }
                task.error_message = f'[RENAME_PENDING] {message}'[:4000] if rename_resume else f'[PROMOTION_PENDING] {message}'[:4000]
                task.status = TransferStatus.RETRY_WAIT
                task.next_run_at = datetime.now(UTC)
                task.locked_at = None
                task.locked_by = None
                await db.flush()
            else:
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
        if rename_resume or promotion_resume:
            logger.warning(
                'transfer task %s will resume from %s; remote write fence preserved',
                task_id,
                'canonical rename' if rename_resume else 'promotion readback',
            )
            return True
        notification_result = await self._notify_failure(
            task_id=task_id,
            task_payload=payload,
            error_message=message,
            resource=resource,
            attempts=getattr(task, 'attempt_count', 1),
            category=str(category) if category else None,
        )
        await self._persist_notification_result(task_id, notification_result)
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
        payload = await self._runtime_final_preflight(
            task_id=task_id,
            resource_id=resource_id,
            payload=payload,
        )
        if payload is None:
            return False
        try:
            outcome = await self.orchestrator.execute(payload)
        except Exception as exc:  # noqa: BLE001 - every remote failure is classified & recorded
            logger.warning('transfer task %s failed (%s): %s', task_id, type(exc).__name__, exc)
            return await self._record_failure(task_id, resource_id, payload, exc)
        return await self._record_success(task_id, resource_id, payload, outcome)
