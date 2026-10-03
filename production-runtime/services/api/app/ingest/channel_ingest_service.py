
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
from app.ingest.media_identity import (
    build_identity_key,
    clean_title,
    extract_explicit_media_type,
    extract_explicit_tmdb_id,
    extract_year,
    is_spam_text,
)
from app.ingest.resource_link_extractor import ResourceLinkExtractor
from app.ingest.url_extractor import extract_urls
from app.models.ingest import ChannelIngestJob, ChannelIngestMessage
from app.models.resource import Resource
from app.models.transfer import TransferJob, TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.schemas.telegram_source import TelegramSourceMessage
from app.transfer.normalization import share_hash
from app.transfer.queue_service import TransferQueueService
from app.transfer.status import REVIEW_STATUS, is_execution_active_task


def preferred_share_url(explicit_url: str | None, urls: list[str]) -> str | None:
    """Prefer an actionable cloud-share URL over metadata/reference links such as TMDb."""
    candidates = [url for url in [explicit_url, *urls] if url]
    return next((url for url in candidates if provider_from_url(url) != 'unknown'), candidates[0] if candidates else None)


class ChannelIngestService:
    @staticmethod
    async def _resolve_tmdb_from_watchlist(db: AsyncSession, *, title: str, season: int | None) -> int | None:
        """Resolve only an unambiguous, currently followed title; never guess from a free-text match."""
        if not title:
            return None
        stmt = select(SeriesWatchlist.tmdb_id).where(
            func.lower(SeriesWatchlist.title) == title.casefold(),
        )
        if season is not None:
            stmt = stmt.where(SeriesWatchlist.season == season)
        ids = {value for value in (await db.scalars(stmt)).all()}
        return next(iter(ids)) if len(ids) == 1 else None

    @staticmethod
    async def _search_tmdb(*, title: str, year: str | None = None, media_type: str | None = None) -> tuple[int | None, str | None]:
        if not title:
            return None, None
        import re as _re
        import socket
        import aiohttp
        from app.core.config import get_settings
        settings = get_settings()
        if not settings.tmdb_api_key:
            return None, None

        q = _re.sub(r"(?i)\bS\d{1,2}\b|第\d{1,2}季", "", title).strip()
        q = _re.sub(r"[\s._\-–—()\[\]（）]+", " ", q).strip()
        q = _re.sub(r"(?i)\b(?:4k|2160p|1080p|web-dl|hdr|dovi|dts|aac|h\.?26[45]|remux)\b", "", q).strip()
        if not q or len(q) < 1:
            return None, None

        if media_type == "tv":
            endpoints = [("tv", "https://api.themoviedb.org/3/search/tv"), ("movie", "https://api.themoviedb.org/3/search/movie")]
        elif media_type == "movie":
            endpoints = [("movie", "https://api.themoviedb.org/3/search/movie"), ("tv", "https://api.themoviedb.org/3/search/tv")]
        else:
            endpoints = [("tv", "https://api.themoviedb.org/3/search/tv"), ("movie", "https://api.themoviedb.org/3/search/movie")]

        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                for kind, url in endpoints:
                    params = {
                        "api_key": settings.tmdb_api_key,
                        "query": q,
                        "language": "zh-CN",
                    }
                    if year and len(str(year)) == 4:
                        if kind == "movie":
                            params["primary_release_year"] = str(year)
                        else:
                            params["first_air_date_year"] = str(year)
                    try:
                        async with session.get(url, params=params) as resp:
                            results = []
                            if resp.status == 200:
                                data = await resp.json()
                                results = data.get("results") or []
                            if not results and (params.get("primary_release_year") or params.get("first_air_date_year")):
                                params.pop("primary_release_year", None)
                                params.pop("first_air_date_year", None)
                                async with session.get(url, params=params) as resp2:
                                    if resp2.status == 200:
                                        data2 = await resp2.json()
                                        results = data2.get("results") or []
                            if results:
                                norm_q = _re.sub(r"[^\w\u4e00-\u9fa5]", "", q).casefold()
                                for item in results:
                                    item_name = item.get("title" if kind == "movie" else "name") or ""
                                    orig_name = item.get("original_title" if kind == "movie" else "original_name") or ""
                                    norm_item = _re.sub(r"[^\w\u4e00-\u9fa5]", "", item_name).casefold()
                                    norm_orig = _re.sub(r"[^\w\u4e00-\u9fa5]", "", orig_name).casefold()
                                    if norm_q and (norm_q == norm_item or norm_q == norm_orig):
                                        return int(item["id"]), kind
                                for item in results:
                                    item_name = item.get("title" if kind == "movie" else "name") or ""
                                    orig_name = item.get("original_title" if kind == "movie" else "original_name") or ""
                                    norm_item = _re.sub(r"[^\w\u4e00-\u9fa5]", "", item_name).casefold()
                                    norm_orig = _re.sub(r"[^\w\u4e00-\u9fa5]", "", orig_name).casefold()
                                    if norm_q and (norm_q in norm_item or norm_item in norm_q):
                                        return int(item["id"]), kind
                    except Exception:
                        pass
        except Exception:
            pass
        return None, None


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
        if channel_setting is None and source_msg.channel_id:
            raw_cid = str(source_msg.channel_id).strip()
            c_cands = [raw_cid]
            if raw_cid.startswith("-100"):
                c_cands.append(raw_cid[4:])
            else:
                c_cands.append(f"-100{raw_cid.lstrip('-')}")
            from app.models.channel import ChannelSetting
            channel_setting = await db.scalar(select(ChannelSetting).where(ChannelSetting.channel_id.in_(c_cands)))
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
            incoming_share = preferred_share_url(source_msg.metadata.get('share_url'), incoming_urls)
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
                    if is_execution_active_task(task) and target_episodes.intersection(canonical_episode_keys(
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
        share_url = preferred_share_url(source_msg.metadata.get('share_url'), urls)
        full_raw_text = f"{source_msg.text or ''} {source_msg.caption or ''}".strip()
        title = source_msg.metadata.get('title') or clean_title(full_raw_text)

        # 1. 垃圾与广告引流过滤熔断
        if not title or is_spam_text(title) or is_spam_text(full_raw_text[:120]):
            job = ChannelIngestJob(channel_id=source_msg.channel_id, message_id=source_msg.message_id,
                                   source_type=source_type, is_forward=source_msg.is_forward,
                                   status=INGEST_SKIPPED, parsed_data=payload, error_message='SKIPPED_SPAM_OR_ADVERTISEMENT')
            db.add(job)
            await db.flush()
            return {'job_id': job.id, 'status': job.status, 'is_forward': job.is_forward, 'skipped': 'spam_advertisement'}

        database_title = title[:512]
        raw_episodes = list(source_msg.metadata.get('episode_keys') or parse_episode_keys(full_raw_text))
        season, _ = parse_season_episode(full_raw_text)
        season = season or source_msg.metadata.get('season')
        episodes = canonical_episode_keys(raw_episodes, season=season)
        explicit_type = extract_explicit_media_type(full_raw_text)

        # 2. 严格的剧集/电影类型判断
        if episodes or explicit_type == 'tv':
            media_type = 'tv'
        elif explicit_type == 'movie':
            media_type = 'movie'
        else:
            media_type = source_msg.metadata.get('media_type') or ('movie' if source_msg.metadata.get('is_movie') else ('tv' if episodes else None))

        tmdb_id = source_msg.metadata.get('tmdb_id') or extract_explicit_tmdb_id(full_raw_text)
        detected_year = source_msg.metadata.get('year') or extract_year(full_raw_text)
        if tmdb_id is None:
            tmdb_id = await ChannelIngestService._resolve_tmdb_from_watchlist(db, title=title, season=season)
        if tmdb_id is None and title:
            tmdb_id, searched_type = await ChannelIngestService._search_tmdb(title=title, year=str(detected_year) if detected_year else None, media_type=media_type)
            if searched_type:
                media_type = searched_type

        # 最终确认：带有剧集特征的一律强锁为 tv
        if episodes or explicit_type == 'tv':
            media_type = 'tv'
        if not media_type:
            media_type = 'movie' if source_msg.metadata.get('is_movie') else 'tv'
        if season is None and episodes:
            season = int(episodes[0].replace('S', '').split('E')[0])
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
        identifiable = bool(tmdb_id and share_url)
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

        # 3. 跨分享链接的 TMDB 级去重与高码率优先覆盖
        from app.transfer.quality_rank import extract_quality_score
        incoming_score = extract_quality_score(full_raw_text)

        if tmdb_id:
            existing_resources = list((await db.scalars(
                select(Resource).where(
                    Resource.tmdb_id == int(tmdb_id),
                    Resource.status.notin_(['FAILED', 'REJECTED', 'INVALID', 'EXPIRED', 'DELETED']),
                )
            )).all())
            target_season = season or 1
            watchlist = await db.scalar(select(SeriesWatchlist).where(
                SeriesWatchlist.tmdb_id == int(tmdb_id),
                SeriesWatchlist.season == target_season,
                SeriesWatchlist.status != 'CANCELLED',
            ))
            if media_type == 'tv' and watchlist is not None and str(watchlist.status).upper() == 'COMPLETED':
                existing_quality_score = 0
                for er in existing_resources:
                    existing_quality_score = max(existing_quality_score, extract_quality_score(f"{er.title or ''} {er.file_names or []}"))
                if not existing_quality_score:
                    inv_rows = list((await db.scalars(select(CloudDiskInventory).where(
                        CloudDiskInventory.tmdb_id == int(tmdb_id),
                        CloudDiskInventory.season == target_season,
                    ))).all())
                    for inv in inv_rows:
                        existing_quality_score = max(existing_quality_score, extract_quality_score(inv.file_name))
                if existing_quality_score >= incoming_score and existing_quality_score > 0:
                    job.status = INGEST_COMPLETED
                    job.identity_status = 'SERIES_ALREADY_COMPLETED'
                    job.ready_for_transfer = False
                    job.transfer_status = 'SKIPPED_EXISTING_QUALITY'
                    await db.flush()
                    return {
                        'job_id': job.id,
                        'status': job.status,
                        'deduplicated': True,
                        'skipped': 'series_already_completed',
                        'is_forward': job.is_forward,
                    }
            if media_type == 'movie' and existing_resources:
                existing_tasks = list((await db.scalars(
                    select(TransferQueueTask).where(
                        TransferQueueTask.resource_id.in_([r.id for r in existing_resources]),
                        TransferQueueTask.status.in_(['QUEUED', 'RUNNING', 'COMPLETED', 'RETRY_WAIT']),
                    )
                )).all())
                for er in existing_resources:
                    er_score = extract_quality_score(f"{er.title or ''} {er.file_names or []}")
                    has_active_or_done = bool(er.transferred_folder_id) or any(t.resource_id == er.id for t in existing_tasks)
                    if has_active_or_done and er_score >= incoming_score:
                        job.status = INGEST_COMPLETED
                        job.identity_status = 'LOWER_OR_EQUAL_QUALITY'
                        job.ready_for_transfer = False
                        job.transfer_status = 'SKIPPED_EXISTING_QUALITY'
                        await db.flush()
                        return {
                            'job_id': job.id,
                            'resource_id': er.id,
                            'status': job.status,
                            'deduplicated': True,
                            'skipped': 'existing_higher_or_equal_quality',
                            'is_forward': job.is_forward,
                        }
            elif media_type == 'tv' and episodes and existing_resources:
                active_or_done_tasks = list((await db.scalars(
                    select(TransferQueueTask).where(
                        TransferQueueTask.resource_id.in_([r.id for r in existing_resources]),
                        TransferQueueTask.status.in_(['QUEUED', 'RUNNING', 'COMPLETED', 'RETRY_WAIT']),
                    )
                )).all())
                for task in active_or_done_tasks:
                    task_eps = set((task.payload or {}).get('episode_keys') or [])
                    if set(episodes).issubset(task_eps):
                        task_score = extract_quality_score(str(task.payload or {}))
                        if task_score >= incoming_score:
                            job.status = INGEST_COMPLETED
                            job.identity_status = 'DUPLICATE_EPISODE_COVERED'
                            job.ready_for_transfer = False
                            job.transfer_status = 'SKIPPED_DUPLICATE_EPISODE'
                            await db.flush()
                            return {
                                'job_id': job.id,
                                'resource_id': task.resource_id,
                                'queue_task_id': task.id,
                                'existing_task_id': task.id,
                                'status': job.status,
                                'deduplicated': True,
                                'queue_reused': True,
                                'is_forward': job.is_forward,
                            }

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
                    **({'selection_mode': 'MISSING_EPISODES' if (episodes and media_type != 'movie') else 'WHOLE_SHARE'}),
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
                enqueue_result.task.status = 'QUEUED'
                enqueue_result.task.error_message = None
                enqueue_result.task.locked_at = None
                enqueue_result.task.locked_by = None
                enqueue_result.task.next_run_at = datetime.now(UTC)
                eq_payload = dict(enqueue_result.task.payload or {})
                eq_payload.pop('preflight_classification', None)
                eq_payload.pop('preflight_reason', None)
                eq_payload['provider'] = 'guangya'
                enqueue_result.task.payload = eq_payload
                job.status = INGEST_COMPLETED
                job.ready_for_transfer = True
                job.transfer_status = 'QUEUED'
                await db.flush()
                return {
                    'job_id': job.id,
                    'resource_id': resource.id,
                    'status': INGEST_COMPLETED,
                    'queued': True,
                    'deduplicated': False,
                    'queue_reused': True,
                    'queued_reactivated': True,
                    'existing_task_id': enqueue_result.task.id,
                    'queue_task_id': enqueue_result.task.id,
                    'preflight_classification': 'AUTO_SAFE',
                    'is_forward': source_msg.is_forward,
                }
        await db.flush()
        return {'job_id': job.id, 'resource_id': resource.id, 'queue_task_id': queue_task_id, 'status': job.status,
                'queued': decision.auto_transfer, 'candidate_switched': candidate_switched, 'is_forward': job.is_forward}
