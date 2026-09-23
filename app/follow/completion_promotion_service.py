"""Strict, no-restore promotion readiness and queue preparation."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.follow.episode_keys import canonical_episode_key
from app.follow.physical_cloud_inventory import PhysicalCloudInventoryScanner
from app.follow.promotion import PromotionDecision, evaluate_promotion
from app.follow.tmdb_provider import TMDBSeasonProvider
from app.models.cloud import CloudConfig, CloudDiskInventory
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.transfer.destination_routing import DestinationRouter
from app.transfer.normalization import build_idempotency_key
from app.transfer.queue_service import TransferQueueService
from app.transfer.status import TransferStatus

_ACTIVE_TRANSFER_STATUSES = frozenset({
    str(TransferStatus.QUEUED),
    str(TransferStatus.RETRY_WAIT),
    str(TransferStatus.RUNNING),
    "PENDING",
})


class CompletionPromotionService:
    @staticmethod
    async def _latest_resource(db: AsyncSession, tmdb_id: int, season: int) -> Resource | None:
        return await db.scalar(select(Resource).where(
            Resource.tmdb_id == tmdb_id,
            Resource.season == season,
            Resource.cloud_name.is_not(None),
        ).order_by(Resource.id.desc()))

    @staticmethod
    async def _tmdb_metadata(resource: Resource) -> dict[str, Any] | None:
        cached = getattr(resource, 'tmdb_metadata', None)
        if isinstance(cached, dict) and cached.get('id'):
            return dict(cached)
        if not resource.tmdb_id:
            return None
        settings = get_settings()
        try:
            return await TMDBSeasonProvider(
                api_key=settings.tmdb_api_key,
                base_url=settings.tmdb_base_url,
            ).fetch_details(int(resource.tmdb_id), str(resource.media_type or 'tv'))
        except Exception:  # noqa: BLE001 - promotion must remain fail-closed
            return None

    @staticmethod
    async def _active_transfer_count(db: AsyncSession, *, tmdb_id: int, season: int) -> int:
        rows = list((await db.scalars(select(TransferQueueTask).where(
            TransferQueueTask.status.in_(_ACTIVE_TRANSFER_STATUSES),
        ))).all())
        if not rows:
            return 0
        resource_ids = {row.resource_id for row in rows if row.resource_id is not None}
        resources = {
            row.id: row
            for row in (await db.scalars(select(Resource).where(Resource.id.in_(resource_ids)))).all()
        } if resource_ids else {}
        count = 0
        for task in rows:
            payload = dict(task.payload or {})
            if str(payload.get("operation") or "").casefold() == "promote":
                continue
            resource = resources.get(task.resource_id)
            row_tmdb = payload.get("tmdb_id") or (resource.tmdb_id if resource else None)
            row_season = payload.get("season") or (resource.season if resource else None)
            try:
                if int(row_tmdb) == int(tmdb_id) and int(row_season) == int(season):
                    count += 1
            except (TypeError, ValueError):
                continue
        return count

    @staticmethod
    async def build_dry_run(
        db: AsyncSession,
        *,
        watchlist: SeriesWatchlist,
        cloud_episode_keys_by_season: Mapping[int, set[str]] | None = None,
        destination_conflict: bool = False,
        promotion_evaluation_id: str | None = None,
        cloud_scan_status: str | None = None,
        cloud_scan_timestamp: str | None = None,
        cloud_scan_watermark: str | None = None,
        require_cloud_scan_watermark: bool = False,
    ) -> PromotionDecision:
        """Build a machine-readable promotion decision from DB and physical evidence.

        ``cloud_episode_keys_by_season`` must come from an authenticated recursive
        provider listing.  Omitting it deliberately fails closed as
        ``INVENTORY_INCOMPLETE``.
        """

        all_rows = list((await db.scalars(select(SeriesWatchlist).where(
            SeriesWatchlist.tmdb_id == watchlist.tmdb_id,
            SeriesWatchlist.status != 'CANCELLED',
        ).order_by(SeriesWatchlist.season.asc(), SeriesWatchlist.id.asc()))).all())
        if not all_rows:
            all_rows = [watchlist]
        inventory_rows = list((await db.scalars(select(CloudDiskInventory).where(
            CloudDiskInventory.tmdb_id == watchlist.tmdb_id,
        ))).all())
        seasons: list[dict[str, Any]] = []
        expected_files: dict[str, list[str]] = {}
        active_total = 0
        for row in all_rows:
            season = int(row.season or 1)
            total = int(row.total_episodes or row.last_aired_episode or 0)
            collected = {
                key
                for value in (row.collected_episodes or [])
                if (key := canonical_episode_key(season, value)) is not None
            }
            inventory = {
                f'S{season:02d}E{int(item.episode):02d}'
                for item in inventory_rows
                if int(item.season or 1) == season and item.episode is not None
            }
            expected_keys = {f'S{season:02d}E{episode:02d}' for episode in range(1, total + 1)} if total > 0 else set()
            physical = None if cloud_episode_keys_by_season is None else {
                key
                for value in (cloud_episode_keys_by_season.get(season) or set())
                if (key := canonical_episode_key(season, value)) is not None
            }
            if physical is not None:
                physical_count = len(physical)
            else:
                physical_count = 0
            files = [
                str(item.file_name).strip()
                for item in inventory_rows
                if int(item.season or 1) == season and str(item.file_name or '').strip()
            ]
            expected_files[f"S{season:02d}"] = sorted(set(files))
            active = await CompletionPromotionService._active_transfer_count(
                db, tmdb_id=watchlist.tmdb_id, season=season,
            )
            active_total += active
            seasons.append({
                "season": season,
                "series_status": row.tmdb_series_status or watchlist.tmdb_series_status,
                "total_expected": total,
                "collected_count": len(collected),
                "inventory_count": len(inventory),
                "cloud_count": physical_count,
                "expected_episode_keys": sorted(expected_keys),
                "collected_episode_keys": sorted(collected),
                "inventory_episode_keys": sorted(inventory),
                "cloud_episode_keys": sorted(physical or set()),
                "physical_evidence_available": physical is not None,
                "active_transfer_count": active,
            })
        resource = await CompletionPromotionService._latest_resource(db, watchlist.tmdb_id, watchlist.season)
        provider = str(resource.cloud_name if resource else 'guangya').lower()
        cloud_config = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
        decision = evaluate_promotion(
            tmdb_id=watchlist.tmdb_id,
            title=watchlist.title,
            series_status=watchlist.tmdb_series_status,
            seasons=seasons,
            ongoing_root=getattr(cloud_config, 'ongoing_target_folder_id', None),
            completed_root=getattr(cloud_config, 'target_folder_id', None),
            active_transfer_count=active_total,
            destination_conflict=destination_conflict,
            expected_files_by_season=expected_files,
            promotion_evaluation_id=promotion_evaluation_id,
            cloud_scan_status=(cloud_scan_status or ("VERIFIED" if cloud_episode_keys_by_season is not None else "SCAN_UNVERIFIED")),
            cloud_scan_timestamp=cloud_scan_timestamp,
            cloud_scan_watermark=cloud_scan_watermark,
            require_cloud_scan_watermark=require_cloud_scan_watermark,
        )
        return decision

    @staticmethod
    async def prefilter_watchlists(db: AsyncSession) -> list[SeriesWatchlist]:
        """Use only PostgreSQL evidence before any physical cloud traversal."""
        rows = list((await db.scalars(select(SeriesWatchlist).where(
            SeriesWatchlist.remote_destination_kind == 'ongoing',
            SeriesWatchlist.remote_series_folder_id.is_not(None),
            SeriesWatchlist.status != 'CANCELLED',
        ).order_by(SeriesWatchlist.tmdb_id.asc(), SeriesWatchlist.season.asc(), SeriesWatchlist.id.asc()))).all())
        candidates: list[SeriesWatchlist] = []
        seen_tmdb: set[int] = set()
        for row in rows:
            if int(row.tmdb_id) in seen_tmdb:
                continue
            group = list((await db.scalars(select(SeriesWatchlist).where(
                SeriesWatchlist.tmdb_id == row.tmdb_id,
                SeriesWatchlist.status != 'CANCELLED',
            ).order_by(SeriesWatchlist.season.asc(), SeriesWatchlist.id.asc()))).all())
            if not group or any(str(item.tmdb_series_status or '').casefold() not in {'ended', 'canceled', 'cancelled'} for item in group):
                continue
            inventory_rows = list((await db.scalars(select(CloudDiskInventory).where(
                CloudDiskInventory.tmdb_id == row.tmdb_id,
            ))).all())
            inventory_by_season: dict[int, int] = {}
            for item in inventory_rows:
                inventory_by_season[int(item.season or 1)] = inventory_by_season.get(int(item.season or 1), 0) + 1
            eligible = True
            active_total = 0
            for item in group:
                season = int(item.season or 1)
                total = int(item.total_episodes or item.last_aired_episode or 0)
                collected = {
                    key for value in item.collected_episodes or []
                    if (key := canonical_episode_key(season, value)) is not None
                }
                active_total += await CompletionPromotionService._active_transfer_count(
                    db, tmdb_id=row.tmdb_id, season=season,
                )
                if total <= 0 or len(collected) < total or inventory_by_season.get(season, 0) < total:
                    eligible = False
            if eligible and active_total == 0:
                candidates.append(row)
                seen_tmdb.add(int(row.tmdb_id))
        return candidates

    @staticmethod
    async def build_physical_dry_run(
        db: AsyncSession,
        *,
        scanner: PhysicalCloudInventoryScanner,
        destination_conflict_reader: Any | None = None,
    ) -> dict[str, Any]:
        """Run bounded physical scans only for cheap DB-prefiltered candidates.

        The method is read-only.  ``destination_conflict_reader`` must itself
        list only completed-root direct children and may return either a bool or
        ``{"conflict": bool, ...}`` diagnostics.
        """
        candidates = await CompletionPromotionService.prefilter_watchlists(db)
        reports: list[dict[str, Any]] = []
        for watchlist in candidates:
            seasons = list((await db.scalars(select(SeriesWatchlist).where(
                SeriesWatchlist.tmdb_id == watchlist.tmdb_id,
                SeriesWatchlist.status != 'CANCELLED',
            ).order_by(SeriesWatchlist.season.asc(), SeriesWatchlist.id.asc()))).all())
            relevant_seasons = [int(item.season or 1) for item in seasons]
            evaluation_id = f"promotion:{watchlist.tmdb_id}:{int(datetime.now(UTC).timestamp())}:{uuid4().hex}"
            scan = await scanner.scan(
                tmdb_id=int(watchlist.tmdb_id),
                series_root_id=str(watchlist.remote_series_folder_id or ''),
                relevant_seasons=relevant_seasons,
            )
            conflict_detail: dict[str, Any] = {"conflict": False, "status": "NOT_RUN"}
            if destination_conflict_reader is not None:
                raw_conflict = destination_conflict_reader(watchlist)
                if hasattr(raw_conflict, '__await__'):
                    raw_conflict = await raw_conflict
                if isinstance(raw_conflict, dict):
                    conflict_detail = dict(raw_conflict)
                else:
                    conflict_detail = {"conflict": bool(raw_conflict), "status": "VERIFIED"}
            decision = await CompletionPromotionService.build_dry_run(
                db,
                watchlist=watchlist,
                cloud_episode_keys_by_season=scan.cloud_episode_keys_by_season,
                destination_conflict=bool(conflict_detail.get('conflict')),
                promotion_evaluation_id=evaluation_id,
                cloud_scan_status=scan.scan_status,
                cloud_scan_timestamp=scan.scan_timestamp,
                cloud_scan_watermark=scan.scan_watermark,
                require_cloud_scan_watermark=True,
            )
            report = decision.as_dict()
            report['physical_scan'] = scan.as_dict()
            report['completed_root_conflict'] = conflict_detail
            reports.append(report)
        return {
            'read_only': True,
            'prefilter_candidates': len(candidates),
            'physical_scans': len(reports),
            'promotion_ready_current_count': sum(1 for item in reports if item.get('decision') == 'PROMOTION_READY'),
            'candidates': reports,
        }

    @staticmethod
    async def enqueue_ready_promotions(
        db: AsyncSession,
        *,
        cloud_episode_keys_by_season: Mapping[int, set[str]] | None = None,
        destination_conflicts: Mapping[int, bool] | None = None,
        physical_scan_results: Mapping[int, Mapping[str, Any]] | None = None,
    ) -> int:
        """Queue only PROMOTION_READY jobs; never queues from TMDB status alone."""

        watchlists = await CompletionPromotionService.prefilter_watchlists(db)
        queued = 0
        for watchlist in watchlists:
            scan = dict((physical_scan_results or {}).get(int(watchlist.tmdb_id)) or {})
            raw_scan_keys = scan.get('cloud_episode_keys_by_season') or cloud_episode_keys_by_season
            scan_keys = {
                int(key): {str(value) for value in values}
                for key, values in (raw_scan_keys or {}).items()
            } if isinstance(raw_scan_keys, Mapping) else raw_scan_keys
            scan_status = scan.get('scan_status')
            scan_timestamp = scan.get('scan_timestamp')
            scan_watermark = scan.get('scan_watermark')
            require_scan = bool(scan)
            decision = await CompletionPromotionService.build_dry_run(
                db,
                watchlist=watchlist,
                cloud_episode_keys_by_season=scan_keys,
                destination_conflict=bool((destination_conflicts or {}).get(watchlist.tmdb_id, False)),
                promotion_evaluation_id=scan.get('promotion_evaluation_id'),
                cloud_scan_status=scan_status,
                cloud_scan_timestamp=scan_timestamp,
                cloud_scan_watermark=scan_watermark,
                require_cloud_scan_watermark=require_scan,
            )
            if decision.decision != 'PROMOTION_READY':
                continue
            resource = await CompletionPromotionService._latest_resource(db, watchlist.tmdb_id, watchlist.season)
            if resource is None:
                continue
            provider = str(resource.cloud_name or '').lower()
            cloud_config = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
            if cloud_config is None or not cloud_config.enabled:
                continue
            metadata = await CompletionPromotionService._tmdb_metadata(resource)
            if metadata is None:
                continue
            route = DestinationRouter.resolve(
                resource=resource,
                cloud_config=cloud_config,
                watchlist=watchlist,
                incoming_episode_keys=[],
                metadata=metadata,
                operation='promote',
                physical_complete=True,
            )
            if route.kind != 'completed' or route.destination is None:
                continue
            destination = route.destination
            promotion_key = build_idempotency_key(resource.id, provider, ['promotion'])
            exists = await db.scalar(select(TransferQueueTask.id).where(
                TransferQueueTask.idempotency_key == promotion_key,
            ))
            if exists is not None:
                continue
            await TransferQueueService.enqueue(
                db,
                resource_id=resource.id,
                provider=provider,
                episode_keys=['promotion'],
                payload={
                    'operation': 'promote',
                    'promotion_status': 'ARCHIVE_PENDING',
                    'promotion_gate': decision.as_dict(),
                    'promotion_expected_files_by_season': decision.as_dict()['expected_files_by_season'],
                    'target_folder_id': route.target_folder_id,
                    'ongoing_root_id': cloud_config.ongoing_target_folder_id,
                    'destination_kind': route.kind,
                    'promotion_source_series_folder_id': watchlist.remote_series_folder_id,
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
                    'tmdb_id': watchlist.tmdb_id,
                    'title': watchlist.title,
                },
            )
            queued += 1
        return queued
