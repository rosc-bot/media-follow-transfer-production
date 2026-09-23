
from datetime import UTC, datetime

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
from app.follow.episode_keys import canonical_episode_key, canonical_episode_keys
from app.ingest.cloud_parser import provider_from_url
from app.ingest.dedup_service import DedupService
from app.ingest.episode_parser import parse_episode_keys, parse_season_episode
from app.ingest.ingest_policy import decide_ingest
from app.ingest.media_identity import build_identity_key, clean_title
from app.ingest.resource_link_extractor import ResourceLinkExtractor
from app.ingest.url_extractor import extract_urls
from app.models.ingest import ChannelIngestJob, ChannelIngestMessage
from app.models.resource import Resource
from app.models.transfer import TransferJob, TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.schemas.telegram_source import TelegramSourceMessage
from app.transfer.normalization import share_hash
from app.transfer.queue_service import TransferQueueService
from app.transfer.status import REVIEW_STATUS


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
    async def _reactivate_pending_task_with_new_evidence(
        db: AsyncSession,
        *,
        resource: Resource,
        episode_keys: list[str],
        share_url: str,
        source_msg: TelegramSourceMessage,
    ) -> TransferQueueTask | None:
        """Requeue one unfenced review task only after exact new source evidence."""
        if resource.status != 'READY' or resource.transferred_folder_id:
            return None
        if not resource.tmdb_id or not resource.season or not resource.share_url:
            return None
        if share_hash(str(resource.share_url)) != share_hash(str(share_url or '')):
            return None
        season = int(resource.season)
        target_keys = set(canonical_episode_keys(episode_keys, season=season))
        if not target_keys:
            return None
        active_transfer = await db.scalar(select(TransferJob.id).where(
            TransferJob.resource_id == resource.id,
            TransferJob.status == 'RUNNING',
        ).limit(1))
        if active_transfer is not None:
            return None
        rows = list((await db.scalars(select(TransferQueueTask).where(
            TransferQueueTask.resource_id == resource.id,
            TransferQueueTask.status == REVIEW_STATUS,
        ).order_by(TransferQueueTask.id.asc()))).all())
        matching = []
        for task in rows:
            payload = dict(task.payload or {})
            task_keys = set(canonical_episode_keys(
                payload.get('episode_keys') or ([resource.episode_key] if resource.episode_key else []),
                season=season,
            ))
            if not target_keys.issubset(task_keys):
                continue
            if str(payload.get('operation') or '').casefold() == 'promote':
                continue
            fenced_stage = str(payload.get('execution_stage') or '').upper()
            if fenced_stage in {'RESTORE_SUBMITTED', 'RESTORED', 'RESTORE_VERIFIED', 'RENAMING', 'RENAME_VERIFIED'}:
                continue
            if any(payload.get(key) for key in ('remote_submission_id', 'restore_task_id', 'restore_submitted_at')):
                continue
            matching.append(task)
        if len(matching) != 1:
            return None
        task = matching[0]
        updated = dict(task.payload or {})
        for key in (
            'preflight_classification', 'preflight_reason', 'preflight_runtime_verified_at',
            'preflight_rename_status', 'selection_snapshot', 'hydrated_selection',
            'selected_file_ids', 'selected_file_names', 'selected_episode_keys',
            'selected_episode_by_file_id', 'batch_presence_preflight',
        ):
            updated.pop(key, None)
        updated.update({
            'provider': str(resource.cloud_name or provider_from_url(share_url) or 'guangya'),
            'resource_id': int(resource.id),
            'tmdb_id': int(resource.tmdb_id),
            'title': str(resource.title or source_msg.metadata.get('title') or ''),
            'season': season,
            'episode_keys': sorted(target_keys),
            'share_url': str(resource.share_url),
            'source_channel_id': str(source_msg.channel_id),
            'source_message_id': int(source_msg.message_id),
            'new_evidence_message_id': int(source_msg.message_id),
        })
        task.payload = updated
        task.status = 'QUEUED'
        task.error_message = None
        task.next_run_at = datetime.now(UTC)
        task.locked_at = None
        task.locked_by = None
        await db.flush()
        return task

    @staticmethod
    async def process_source_message(db: AsyncSession, source_msg: TelegramSourceMessage, *, channel_setting: object | None = None) -> dict:
        source_type = source_msg.source_type.strip().lower()
        if source_type not in SOURCE_TYPES:
            raise ValueError(f'unsupported source_type: {source_type}')
        decision = decide_ingest(source_type=source_type, is_forward=source_msg.is_forward, setting=channel_setting)
        existing = await db.scalar(
            select(ChannelIngestJob).where(
                ChannelIngestJob.channel_id == source_msg.channel_id,
                ChannelIngestJob.message_id == source_msg.message_id,
            )
        )
        new_evidence = existing is None
        if existing:
            existing_parsed = dict(existing.parsed_data or {})
            incoming_hash = str(source_msg.metadata.get('resource_content_hash') or '').strip()
            previous_hash = str(existing_parsed.get('resource_content_hash') or '').strip()
            incoming_tmdb = source_msg.metadata.get('tmdb_id')
            identity_changed = bool(incoming_tmdb and (existing.tmdb_id is None or int(existing.tmdb_id) != int(incoming_tmdb)))
            content_changed = bool(incoming_hash and incoming_hash != previous_hash)
            new_evidence = content_changed or identity_changed
            # A Telegram message may contain a season pack and Scout invokes
            # ingest once per missing episode using the same source message.
            # Reuse is message+episode scoped, not message scoped.
            old_episodes = set(canonical_episode_keys(existing.detected_episodes or [], season=existing.season))
            new_episodes = set(canonical_episode_keys(
                source_msg.metadata.get('episode_keys') or parse_episode_keys(f'{source_msg.text} {source_msg.caption}'),
                season=source_msg.metadata.get('season') or existing.season,
            ))
            additional_episodes = new_episodes - old_episodes
            incoming_urls = extract_urls(
                f'{source_msg.text} {source_msg.caption}',
                source_msg.entities,
                source_msg.button_urls,
            )
            incoming_urls.extend(url for url in source_msg.urls if url not in incoming_urls)
            incoming_share = source_msg.metadata.get('share_url') or (incoming_urls[0] if incoming_urls else None)
            same_share = bool(incoming_share and share_hash(incoming_share) == existing.share_hash)
            if not additional_episodes and same_share and not new_evidence:
                target_episodes = set(new_episodes)
                source_resources = select(Resource.id).where(
                    Resource.source_channel_id == source_msg.channel_id,
                    Resource.source_message_id == source_msg.message_id,
                    Resource.share_url == existing.share_url,
                )
                active_tasks = list((await db.scalars(
                    select(TransferQueueTask).where(
                        TransferQueueTask.resource_id.in_(source_resources),
                        TransferQueueTask.status.in_(['QUEUED', 'RUNNING', 'RETRY_WAIT']),
                    )
                )).all())
                reuse_task = next((
                    task for task in active_tasks
                    if target_episodes.intersection(canonical_episode_keys(
                        (task.payload or {}).get('episode_keys') or [], season=existing.season,
                    ))
                ), None)
                if reuse_task is not None:
                    return {
                        'job_id': existing.id,
                        'resource_id': reuse_task.resource_id,
                        'queue_task_id': reuse_task.id,
                        'existing_task_id': reuse_task.id,
                        'status': existing.status,
                        'is_forward': existing.is_forward,
                        'deduplicated': True,
                        'queue_reused': True,
                        'transfer_status': existing.transfer_status,
                        'preflight_classification': (reuse_task.payload or {}).get('preflight_classification'),
                        'queued_reactivated': bool((reuse_task.payload or {}).get('queued_reactivated')),
                        'candidate_switched': bool((reuse_task.payload or {}).get('candidate_switched')),
                        'stale_pending_recovered': bool((reuse_task.payload or {}).get('stale_pending_recovered')),
                    }
                pending_tasks = list((await db.scalars(
                    select(TransferQueueTask).where(
                        TransferQueueTask.resource_id.in_(source_resources),
                        TransferQueueTask.status == REVIEW_STATUS,
                    )
                )).all())
                review_task = next((
                    task for task in pending_tasks
                    if target_episodes.intersection(canonical_episode_keys(
                        (task.payload or {}).get('episode_keys') or [], season=existing.season,
                    ))
                ), None)
                if review_task is not None:
                    review_payload = dict(review_task.payload or {})
                    return {
                        'job_id': existing.id,
                        'resource_id': review_task.resource_id,
                        'queue_task_id': review_task.id,
                        'existing_task_id': review_task.id,
                        'status': INGEST_NEEDS_REVIEW,
                        'is_forward': existing.is_forward,
                        'queued': False,
                        'deduplicated': False,
                        'queue_reused': False,
                        'transfer_status': REVIEW_STATUS,
                        'preflight_classification': review_payload.get('preflight_classification') or 'NEEDS_REVIEW',
                        'preflight_reason': review_payload.get('preflight_reason'),
                        'queued_reactivated': bool(review_payload.get('queued_reactivated')),
                        'candidate_switched': bool(review_payload.get('candidate_switched')),
                        'stale_pending_recovered': bool(review_payload.get('stale_pending_recovered')),
                    }
                return {
                    'job_id': existing.id,
                    'resource_id': None,
                    'status': existing.status,
                    'is_forward': existing.is_forward,
                    'deduplicated': True,
                    'queue_reused': False,
                    'transfer_status': existing.transfer_status,
                }
            # Ingest only newly requested episode(s); never re-enqueue episodes
            # already represented by this source message/job.
            episode_keys_to_process = additional_episodes if additional_episodes else (new_episodes if new_evidence else set())
            if episode_keys_to_process:
                source_msg = source_msg.model_copy(update={
                    'metadata': {**source_msg.metadata, 'episode_keys': sorted(episode_keys_to_process)},
                })
        payload = source_msg.model_dump(mode='json')
        urls = extract_urls(f'{source_msg.text} {source_msg.caption}', source_msg.entities, source_msg.button_urls)
        urls.extend(x for x in source_msg.urls if x not in urls)
        share_url = source_msg.metadata.get('share_url') or (urls[0] if urls else None)
        raw_episodes = list(source_msg.metadata.get('episode_keys') or parse_episode_keys(f'{source_msg.text} {source_msg.caption}'))
        season, _ = parse_season_episode(f'{source_msg.text} {source_msg.caption}')
        season = season or source_msg.metadata.get('season')
        episodes = canonical_episode_keys(raw_episodes, season=season)
        tmdb_id = source_msg.metadata.get('tmdb_id')
        title = source_msg.metadata.get('title') or clean_title(source_msg.text or source_msg.caption)
        database_title = title[:512]
        if tmdb_id is None:
            tmdb_id = await ChannelIngestService._resolve_tmdb_from_watchlist(db, title=title, season=season)
        media_type = source_msg.metadata.get('media_type') or ('movie' if source_msg.metadata.get('is_movie') else 'tv')
        resource_content_hash = str(source_msg.metadata.get('resource_content_hash') or '').strip()
        parsed = {**payload, 'urls': urls, 'share_url': share_url, 'source_type': source_type,
                  'is_forward': bool(source_msg.is_forward), 'episode_keys': episodes,
                  'tmdb_id': tmdb_id, 'title': title, 'media_type': media_type,
                  'resource_content_hash': resource_content_hash or None,
                  'transfer_mode': 'AUTO' if decision.auto_transfer else 'MANUAL'}
        message_row = await db.scalar(select(ChannelIngestMessage).where(
            ChannelIngestMessage.channel_id == source_msg.channel_id,
            ChannelIngestMessage.message_id == source_msg.message_id,
        ))
        if message_row is None:
            message_row = ChannelIngestMessage(channel_id=source_msg.channel_id, message_id=source_msg.message_id,
                                               source_type=source_type, is_forward=source_msg.is_forward,
                                               payload=parsed, status='QUEUED')
            db.add(message_row)
        else:
            prior_payload = dict(message_row.payload or {})
            prior_episodes = set(canonical_episode_keys(
                prior_payload.get('episode_keys') or [], season=season,
            ))
            message_row.payload = {**prior_payload, **parsed,
                                   'episode_keys': sorted(prior_episodes | set(episodes))}
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
        job = await db.scalar(select(ChannelIngestJob).where(
            ChannelIngestJob.channel_id == source_msg.channel_id,
            ChannelIngestJob.message_id == source_msg.message_id,
            ChannelIngestJob.share_hash == digest,
        ))
        if job is None:
            job = ChannelIngestJob(channel_id=source_msg.channel_id, message_id=source_msg.message_id,
                                   source_type=source_type, is_forward=source_msg.is_forward, share_url=share_url,
                                   share_hash=digest, status=INGEST_NEEDS_REVIEW, parsed_data=parsed,
                                   media_type=media_type, tmdb_id=tmdb_id, title=database_title, season=season,
                                   detected_episodes=episodes)
            db.add(job)
            await db.flush()
        else:
            prior_episodes = set(canonical_episode_keys(job.detected_episodes or [], season=season))
            combined_episodes = sorted(prior_episodes | set(episodes))
            job.detected_episodes = combined_episodes
            job.parsed_data = {**dict(job.parsed_data or {}), **parsed, 'episode_keys': combined_episodes}
            job.tmdb_id = int(tmdb_id) if tmdb_id is not None else job.tmdb_id
            job.title = database_title or job.title
            job.season = season or job.season
            await db.flush()
        episodes = sorted(set(episodes))
        identifiable = bool(tmdb_id and share_url and (episodes or media_type == 'movie'))
        if not identifiable:
            job.error_message = 'missing tmdb_id or season/episode identity'
            await db.flush()
            return {'job_id': job.id, 'status': job.status, 'is_forward': job.is_forward}
        identity = build_identity_key(tmdb_id=tmdb_id, season=season, episodes=episodes, share_hash=digest,
                                      version_key=str(source_msg.metadata.get('version_key') or ''))
        resource = await DedupService.find_existing(db, identity)
        if resource:
            pending_task = None
            if new_evidence:
                pending_task = await ChannelIngestService._reactivate_pending_task_with_new_evidence(
                    db,
                    resource=resource,
                    episode_keys=episodes,
                    share_url=share_url,
                    source_msg=source_msg,
                )
            if pending_task is not None:
                job.status = INGEST_COMPLETED
                job.identity_status = 'IDENTIFIED'
                job.ready_for_transfer = True
                job.transfer_status = 'QUEUED'
                job.error_message = None
                job.parsed_data = {**dict(job.parsed_data or {}), **parsed}
                await db.flush()
                return {
                    'job_id': job.id,
                    'resource_id': resource.id,
                    'queue_task_id': pending_task.id,
                    'existing_task_id': pending_task.id,
                    'status': job.status,
                    'queued': True,
                    'deduplicated': True,
                    'queue_reused': True,
                    'queued_reactivated': True,
                    'stale_pending_recovered': True,
                    'is_forward': job.is_forward,
                }
            job.status = INGEST_COMPLETED; job.identity_status = 'DUPLICATE'; job.ready_for_transfer = False
            job.transfer_status = 'SKIPPED_DUPLICATE'; await db.flush()
            return {'job_id': job.id, 'resource_id': resource.id, 'status': job.status, 'deduplicated': True, 'is_forward': job.is_forward}
        resource = Resource(identity_key=identity, tmdb_id=int(tmdb_id), title=database_title, media_type=media_type,
                            year=source_msg.metadata.get('year'), season=season,
                            episode=int(episodes[0].split('E')[1]) if len(episodes) == 1 and 'E' in episodes[0] else None,
                            episode_key=canonical_episode_key(season, episodes[0]) if len(episodes) == 1 else None,
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
        queue_task_id = None
        candidate_switched = False
        if decision.auto_transfer:
            enqueue_result = await TransferQueueService.enqueue_with_result(
                db,
                resource_id=resource.id,
                provider=resource.cloud_name or 'guangya',
                episode_keys=episodes,
                payload={
                    'resource_id': resource.id,
                    'share_url': share_url,
                    'expected_files': resource.file_names,
                    'episode_keys': episodes,
                    'tmdb_id': int(tmdb_id),
                    'title': title,
                    'season': season,
                    'source_type': source_type,
                    'source_channel_id': source_msg.channel_id,
                    'source_message_id': source_msg.message_id,
                    'is_forward': source_msg.is_forward,
                    'notification_chat_id': source_msg.metadata.get('notification_chat_id'),
                    **({'selection_mode': 'MISSING_EPISODES'} if len(episodes) > 1 else {}),
                },
            )
            queue_task_id = enqueue_result.task.id
            if enqueue_result.created:
                stale_rows = list((await db.execute(
                    select(TransferQueueTask, Resource)
                    .join(Resource, Resource.id == TransferQueueTask.resource_id)
                    .where(
                        TransferQueueTask.status == REVIEW_STATUS,
                        Resource.tmdb_id == int(tmdb_id),
                        Resource.season == int(season or 1),
                    )
                )).all())
                target_keys = set(canonical_episode_keys(episodes, season=season))
                for stale_task, stale_resource in stale_rows:
                    old_payload = dict(stale_task.payload or {})
                    old_keys = set(canonical_episode_keys(
                        old_payload.get('episode_keys') or ([stale_resource.episode_key] if stale_resource.episode_key else []),
                        season=season,
                    ))
                    if (
                        target_keys.intersection(old_keys)
                        and stale_resource.share_url
                        and share_hash(stale_resource.share_url) != digest
                    ):
                        candidate_switched = True
                        break
            if enqueue_result.deduplicated:
                job.ready_for_transfer = False
                job.transfer_status = 'SKIPPED_DUPLICATE'
                return {
                    'job_id': job.id,
                    'resource_id': resource.id,
                    'status': job.status,
                    'deduplicated': True,
                    'deduplicated_existing_task': True,
                    'queue_reused': False,
                    'existing_task_id': enqueue_result.task.id,
                    'queue_task_id': enqueue_result.task.id,
                    'is_forward': source_msg.is_forward,
                }
            if enqueue_result.reused:
                return {
                    'job_id': job.id,
                    'resource_id': resource.id,
                    'status': job.status,
                    'queued': True,
                    'deduplicated': False,
                    'deduplicated_existing_task': False,
                    'queue_reused': True,
                    'existing_task_id': enqueue_result.task.id,
                    'queue_task_id': enqueue_result.task.id,
                    'is_forward': source_msg.is_forward,
                    'preflight_classification': (enqueue_result.task.payload or {}).get('preflight_classification'),
                    'queued_reactivated': bool((enqueue_result.task.payload or {}).get('queued_reactivated')),
                    'candidate_switched': bool((enqueue_result.task.payload or {}).get('candidate_switched')),
                    'stale_pending_recovered': bool((enqueue_result.task.payload or {}).get('stale_pending_recovered')),
                }
            if str(enqueue_result.task.status) == REVIEW_STATUS:
                job.status = INGEST_NEEDS_REVIEW
                job.ready_for_transfer = False
                job.transfer_status = REVIEW_STATUS
                job.error_message = 'existing review task requires a new candidate or cloud-state change'
                await db.flush()
                return {
                    'job_id': job.id,
                    'resource_id': resource.id,
                    'status': INGEST_NEEDS_REVIEW,
                    'queued': False,
                    'deduplicated': False,
                    'queue_reused': False,
                    'existing_task_id': enqueue_result.task.id,
                    'queue_task_id': enqueue_result.task.id,
                    'preflight_classification': 'NEEDS_REVIEW',
                    'preflight_reason': 'EXISTING_PENDING_REQUIRES_NEW_EVIDENCE',
                    'is_forward': source_msg.is_forward,
                }
        await db.flush()
        return {'job_id': job.id, 'resource_id': resource.id, 'queue_task_id': queue_task_id, 'status': job.status,
                'queued': decision.auto_transfer, 'candidate_switched': candidate_switched, 'is_forward': job.is_forward}
