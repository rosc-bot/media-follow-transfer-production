"""Queue safe no-restore promotions for series that have become complete."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.cloud import CloudConfig
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.transfer.destination_routing import DestinationRouter
from app.transfer.normalization import build_idempotency_key
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import _series_layout_names


class CompletionPromotionService:
    @staticmethod
    async def enqueue_ready_promotions(db: AsyncSession) -> int:
        """Queue each known ongoing remote root once after TMDB completion is proven.

        A promotion reuses the latest resource solely as an immutable identity and
        provider anchor.  It never calls restore_share or creates a new resource.
        """
        watchlists = list((await db.scalars(select(SeriesWatchlist).where(
            SeriesWatchlist.remote_destination_kind == 'ongoing',
            SeriesWatchlist.remote_series_folder_id.is_not(None),
        ))).all())
        queued = 0
        for watchlist in watchlists:
            resource = await db.scalar(select(Resource).where(
                Resource.tmdb_id == watchlist.tmdb_id,
                Resource.season == watchlist.season,
                Resource.cloud_name.is_not(None),
            ).order_by(Resource.id.desc()))
            if resource is None:
                continue
            provider = str(resource.cloud_name or '').lower()
            cloud_config = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
            if cloud_config is None or not cloud_config.enabled:
                continue
            route = DestinationRouter.resolve(
                resource=resource,
                cloud_config=cloud_config,
                watchlist=watchlist,
                incoming_episode_keys=[],
            )
            if route.kind != 'completed':
                continue
            layout = _series_layout_names(resource, watchlist, route.kind)
            if layout is None:
                continue
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
                    'target_folder_id': route.target_folder_id,
                    'destination_kind': route.kind,
                    'promotion_source_series_folder_id': watchlist.remote_series_folder_id,
                    'series_folder_name': layout[0],
                    'season_folder_name': layout[1],
                },
            )
            queued += 1
        return queued
