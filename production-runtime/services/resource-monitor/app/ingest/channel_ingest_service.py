
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import (
    INGEST_COMPLETED,
    INGEST_NEEDS_REVIEW,
    INGEST_SKIPPED,
    SOURCE_MANUAL_FORWARD,
    SOURCE_TELEGRAM_CHANNEL,
    SOURCE_TYPES,
)
from app.ingest.cloud_parser import provider_from_url
from app.ingest.dedup_service import DedupService
from app.ingest.episode_parser import parse_episode_keys, parse_season_episode
from app.ingest.ingest_policy import decide_ingest
from app.ingest.media_identity import build_identity_key, clean_title
from app.ingest.resource_link_extractor import ResourceLinkExtractor
from app.ingest.url_extractor import extract_urls
from app.models.ingest import ChannelIngestJob, ChannelIngestMessage
from app.models.resource import Resource
from app.models.watchlist import SeriesWatchlist
from app.schemas.telegram_source import TelegramSourceMessage
from app.transfer.normalization import share_hash
from app.transfer.queue_service import TransferQueueService


class ChannelIngestService:
    @staticmethod
    async def _resolve_tmdb_from_watchlist(db: AsyncSession, *, title: str, season: int | None) -> int | None:
        """Resolve only an unambiguous, currently followed title; never guess from a free-text match."""
        if not title:
            return None
        stmt = select(SeriesWatchlist.tmdb_id).where(
            func.lower(SeriesWatchlist.title) == title.casefold(),
            SeriesWatchlist.status == 'FOLLOWING',
        )
        if season is not None:
            stmt = stmt.where(SeriesWatchlist.season == season)
        ids = {value for value in (await db.scalars(stmt)).all()}
        return next(iter(ids)) if len(ids) == 1 else None

    @staticmethod
    async def process_source_message(db: AsyncSession, source_msg: TelegramSourceMessage, *, channel_setting: object | None = None) -> dict:
        source_type = source_msg.source_type.strip().lower()
        if source_type not in SOURCE_TYPES:
            raise ValueError(f'unsupported source_type: {source_type}')
        decision = decide_ingest(source_type=source_type, is_forward=source_msg.is_forward, setting=channel_setting)
        existing = await db.scalar(select(ChannelIngestJob).where(ChannelIngestJob.channel_id == source_msg.channel_id, ChannelIngestJob.message_id == source_msg.message_id))
        if existing:
            queue_reused = existing.transfer_status == 'QUEUED'
            return {
                'job_id': existing.id,
                'status': existing.status,
                'is_forward': existing.is_forward,
                'deduplicated': True,
                'queue_reused': queue_reused,
                'transfer_status': existing.transfer_status,
            }
        payload = source_msg.model_dump(mode='json')
        urls = extract_urls(f'{source_msg.text} {source_msg.caption}', source_msg.entities, source_msg.button_urls)
        urls.extend(x for x in source_msg.urls if x not in urls)
        share_url = source_msg.metadata.get('share_url') or (urls[0] if urls else None)
        episodes = list(source_msg.metadata.get('episode_keys') or parse_episode_keys(f'{source_msg.text} {source_msg.caption}'))
        season, _ = parse_season_episode(f'{source_msg.text} {source_msg.caption}')
        tmdb_id = source_msg.metadata.get('tmdb_id')
        title = source_msg.metadata.get('title') or clean_title(source_msg.text or source_msg.caption)
        if tmdb_id is None:
            tmdb_id = await ChannelIngestService._resolve_tmdb_from_watchlist(db, title=title, season=season)
        media_type = source_msg.metadata.get('media_type') or ('movie' if source_msg.metadata.get('is_movie') else 'tv')
        parsed = {**payload, 'urls': urls, 'share_url': share_url, 'source_type': source_type,
                  'is_forward': bool(source_msg.is_forward), 'episode_keys': episodes,
                  'tmdb_id': tmdb_id, 'title': title, 'media_type': media_type,
                  'transfer_mode': 'AUTO' if decision.auto_transfer else 'MANUAL'}
        db.add(ChannelIngestMessage(channel_id=source_msg.channel_id, message_id=source_msg.message_id,
                                    source_type=source_type, is_forward=source_msg.is_forward,
                                    payload=parsed, status='QUEUED'))
        await db.flush()
        if not decision.accepted:
            job = ChannelIngestJob(channel_id=source_msg.channel_id, message_id=source_msg.message_id,
                                   source_type=source_type, is_forward=source_msg.is_forward,
                                   status=INGEST_NEEDS_REVIEW, parsed_data={**parsed, 'rejection_reason': decision.reason})
            db.add(job); await db.flush()
            return {'job_id': job.id, 'status': job.status, 'is_forward': job.is_forward, 'reason': decision.reason}
        if not share_url:
            job = ChannelIngestJob(channel_id=source_msg.channel_id, message_id=source_msg.message_id,
                                   source_type=source_type, is_forward=source_msg.is_forward,
                                   status=INGEST_SKIPPED, parsed_data=parsed, error_message='SKIPPED_NO_RESOURCE_LINK')
            db.add(job); await db.flush()
            return {'job_id': job.id, 'status': job.status, 'is_forward': job.is_forward,
                    'skipped': 'no_share_url'}
        if source_type in (SOURCE_TELEGRAM_CHANNEL, SOURCE_MANUAL_FORWARD) and \
                not ResourceLinkExtractor.has_supported_share_url(urls or [share_url]):
            job = ChannelIngestJob(channel_id=source_msg.channel_id, message_id=source_msg.message_id,
                                   source_type=source_type, is_forward=source_msg.is_forward,
                                   status=INGEST_SKIPPED, parsed_data=parsed,
                                   error_message='SKIPPED_UNSUPPORTED_RESOURCE_LINK')
            db.add(job); await db.flush()
            return {'job_id': job.id, 'status': job.status, 'is_forward': job.is_forward,
                    'skipped': 'unsupported_resource_link'}
        digest = share_hash(share_url)
        job = ChannelIngestJob(channel_id=source_msg.channel_id, message_id=source_msg.message_id,
                               source_type=source_type, is_forward=source_msg.is_forward, share_url=share_url,
                               share_hash=digest, status=INGEST_NEEDS_REVIEW, parsed_data=parsed,
                               media_type=media_type, tmdb_id=tmdb_id, title=title, season=season,
                               detected_episodes=episodes)
        db.add(job); await db.flush()
        identifiable = bool(tmdb_id and share_url and (episodes or media_type == 'movie'))
        if not identifiable:
            job.error_message = 'missing tmdb_id or season/episode identity'
            await db.flush()
            return {'job_id': job.id, 'status': job.status, 'is_forward': job.is_forward}
        identity = build_identity_key(tmdb_id=tmdb_id, season=season, episodes=episodes, share_hash=digest,
                                      version_key=str(source_msg.metadata.get('version_key') or ''))
        resource = await DedupService.find_existing(db, identity)
        if resource:
            job.status = INGEST_COMPLETED; job.identity_status = 'DUPLICATE'; job.ready_for_transfer = False
            job.transfer_status = 'SKIPPED_DUPLICATE'; await db.flush()
            return {'job_id': job.id, 'resource_id': resource.id, 'status': job.status, 'deduplicated': True, 'is_forward': job.is_forward}
        resource = Resource(identity_key=identity, tmdb_id=int(tmdb_id), title=title, media_type=media_type,
                            year=source_msg.metadata.get('year'), season=season,
                            episode=int(episodes[0].split('E')[1]) if len(episodes) == 1 and 'E' in episodes[0] else None,
                            episode_key=episodes[0] if len(episodes) == 1 else None,
                            version_key=str(source_msg.metadata.get('version_key') or ''),
                            cloud_name=provider_from_url(share_url), share_url=share_url,
                            source_type=source_type, source_channel_id=source_msg.channel_id,
                            source_message_id=source_msg.message_id, file_names=list(source_msg.metadata.get('file_names') or []))
        try:
            async with db.begin_nested():
                db.add(resource)
                await db.flush()
        except IntegrityError:
            # A concurrently-ingested, live candidate won the partial unique
            # index. Re-check and retain normal duplicate semantics instead of
            # leaking an integrity error to the monitor worker.
            resource = await DedupService.find_existing(db, identity)
            if resource is None:
                raise
            job.status = INGEST_COMPLETED; job.identity_status = 'DUPLICATE'; job.ready_for_transfer = False
            job.transfer_status = 'SKIPPED_DUPLICATE'; await db.flush()
            return {'job_id': job.id, 'resource_id': resource.id, 'status': job.status, 'deduplicated': True, 'is_forward': job.is_forward}
        job.status = INGEST_COMPLETED; job.identity_status = 'IDENTIFIED'; job.ready_for_transfer = True
        job.transfer_status = 'QUEUED' if decision.auto_transfer else 'MANUAL'
        if decision.auto_transfer:
            await TransferQueueService.enqueue(db, resource_id=resource.id, provider=resource.cloud_name or 'guangya',
                episode_keys=episodes, payload={'resource_id': resource.id, 'share_url': share_url,
                'expected_files': resource.file_names, 'episode_keys': episodes, 'source_type': source_type, 'is_forward': source_msg.is_forward})
        await db.flush()
        return {'job_id': job.id, 'resource_id': resource.id, 'status': job.status,
                'queued': decision.auto_transfer, 'is_forward': job.is_forward}
