"""Alternative-resource switching for failed transfer tasks (Phase 2C §六).

When a transfer task fails, the operator may ask the bot to *switch
resource*. The switch now runs on the persistent candidate ledger
(``resource_candidates``):

1. locate the current tmdb_id + season + episode from the task payload;
2. update the current candidate's status according to the actual failure
   category (permanent vs temporary vs auth);
3. query the other usable candidates from the ledger;
4. exclude permanently-dead candidates (INVALID_SHARE / NO_VIDEO /
   EPISODE_MISMATCH);
5. prefer VALIDATED over DISCOVERED;
6. if the ledger has nothing left, re-run Scout and persist its new
   candidates;
7. re-select from the refreshed ledger.

A→A is impossible: the current share hash and source message are excluded,
and permanently-dead candidates can never be re-selected.  With no
alternative, raises :class:`NoAlternativeResourceError` (the bot answers
"暂未找到其它资源" instead of an unknown error).
"""

from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.scout.message_search import MessageSearch
from app.transfer.candidate_service import (
    get_candidates_for_episode,
    mark_candidate_failure,
    record_candidate,
    select_alternative_candidate,
    stable_share_key,
)
from app.transfer.queue_service import TransferQueueService

logger = logging.getLogger(__name__)


class NoAlternativeResourceError(RuntimeError):
    """No other usable candidate exists for this tmdb/season/episode."""


async def _current_candidate(
    db: AsyncSession,
    *,
    tmdb_id: int,
    season: int,
    episode_key: str,
    share_hash_value: str | None,
    source_message_id: int | None,
):
    """Locate the candidate row that corresponds to the failing task."""
    kwargs: dict = {'tmdb_id': tmdb_id, 'season': season, 'episode_key': episode_key}
    if share_hash_value:
        candidates = await get_candidates_for_episode(db, **kwargs)
        for candidate in candidates:
            if candidate.share_hash == share_hash_value:
                return candidate
    if source_message_id:
        candidates = await get_candidates_for_episode(db, **kwargs)
        for candidate in candidates:
            if candidate.source_message_id == source_message_id:
                return candidate
    return None


async def switch_resource(
    db: AsyncSession,
    *,
    task_id: int,
    resource_db_path: str,
    exclude_share_hash: str | None = None,
    exclude_source_message_id: int | None = None,
    category: str | None = None,
) -> dict:
    """Find and enqueue an alternative resource for a failed task."""
    task = await db.get(TransferQueueTask, task_id)
    if task is None:
        raise NoAlternativeResourceError(f'task #{task_id} does not exist')
    payload = dict(task.payload or {})
    resource = await db.get(Resource, task.resource_id) if task.resource_id else None

    tmdb_id = payload.get('tmdb_id')
    season = payload.get('season')
    title = payload.get('title') or (resource.title if resource else None)
    episode_keys = list(payload.get('episode_keys') or ([resource.episode_key] if resource and resource.episode_key else []))
    source_channel = payload.get('source_channel_id') or (resource.source_channel_id if resource else None)
    source_msg = payload.get('source_message_id') or (resource.source_message_id if resource else None)
    current_share_url = payload.get('share_url') or (resource.share_url if resource else None)

    current_share_hash = exclude_share_hash or (
        stable_share_key(str(current_share_url)) if current_share_url else None
    )
    if exclude_source_message_id is not None:
        source_msg = exclude_source_message_id

    if not tmdb_id or not title or not episode_keys:
        raise NoAlternativeResourceError('task payload lacks tmdb_id/title/episode_keys needed to re-scout')

    # 2. Update the current candidate's status by the real failure category.
    for episode_key in episode_keys:
        current_candidate = await _current_candidate(
            db,
            tmdb_id=int(tmdb_id),
            season=int(season or 1),
            episode_key=episode_key,
            share_hash_value=current_share_hash,
            source_message_id=int(source_msg or 0) if source_msg else None,
        )
        if current_candidate is not None:
            await mark_candidate_failure(
                db,
                candidate=current_candidate,
                category=category,
                failure_reason=f'switched away from task #{task_id}',
            )
    # Also mark the Resource row dead (backward compat with existing flows).
    if resource is not None and resource.status not in ('INVALID', 'NO_VIDEO', 'FAILED', 'REJECTED', 'EXPIRED'):
        resource.status = 'FAILED'
        try:
            await db.flush()
        except Exception:  # noqa: BLE001 - non-fatal bookkeeping
            await db.rollback()

    # 3/4/5. Query the ledger first (excludes permanent failures automatically).
    chosen_by_episode: dict[str, dict] = {}
    for episode_key in episode_keys:
        candidate = await select_alternative_candidate(
            db,
            tmdb_id=int(tmdb_id),
            season=int(season or 1),
            episode_key=episode_key,
            exclude_share_hash=current_share_hash,
            exclude_source_message_id=int(source_msg or 0) if source_msg else None,
        )
        if candidate is not None:
            chosen_by_episode[episode_key] = {
                'candidate': candidate,
                'share_url': candidate.share_url,
                'source_channel_id': candidate.source_channel_id,
                'source_message_id': candidate.source_message_id,
                'source': 'LEDGER',
            }

    # 6. Ledger empty for some episodes → re-run Scout, persist new candidates.
    missing_from_ledger = [ep for ep in episode_keys if ep not in chosen_by_episode]
    if missing_from_ledger:
        await _rescout_and_persist(
            db,
            tmdb_id=int(tmdb_id),
            title=title,
            season=int(season or 1),
            episode_keys=missing_from_ledger,
            resource_db_path=resource_db_path,
            current_share_hash=current_share_hash,
            source_msg=source_msg,
            chosen_by_episode=chosen_by_episode,
        )

    if not chosen_by_episode:
        raise NoAlternativeResourceError(
            f'未找到《{title}》S{int(season or 1):02d} 的其它可用资源'
        )

    # 7/8. Enqueue new tasks for the chosen alternatives (idempotent per share).
    # NOTE: idempotency_key is keyed on resource_id, so a new Resource gets a
    # fresh key even for the same share — dedup here by actual task payload
    # (tmdb_id + episodes + share_url) so an already-queued alternative is
    # reused instead of duplicated.
    active_statuses = ('QUEUED', 'RETRY_WAIT', 'RUNNING', 'PENDING')
    active_tasks = (await db.execute(
        select(TransferQueueTask).where(TransferQueueTask.status.in_(active_statuses))
    )).scalars().all()
    new_tasks: list[dict] = []
    for episode_key, pick in chosen_by_episode.items():
        chosen_url = str(pick['share_url'])
        chosen_source_channel = pick.get('source_channel_id') or source_channel or ''
        chosen_source_msg = pick.get('source_message_id') or int(source_msg or 0) or None
        provider = 'guangya'
        if 'quark' in chosen_url:
            provider = 'quark'
        elif 'baidu' in chosen_url:
            provider = 'baidu'
        elif '115' in chosen_url:
            provider = '115'
        elif 'alipan' in chosen_url or 'aliyundrive' in chosen_url:
            provider = 'aliyun'
        existing = next(
            (t for t in active_tasks
             if str((t.payload or {}).get('share_url') or '') == chosen_url
             and int((t.payload or {}).get('tmdb_id') or 0) == int(tmdb_id)
             and str(episode_key) in {str(e) for e in (t.payload or {}).get('episode_keys') or []}),
            None,
        )
        if existing:
            new_tasks.append({'episode_key': episode_key, 'task_id': existing.id, 'status': existing.status, 'reused': True})
            continue
        new_resource = Resource(
            identity_key=f'{int(tmdb_id)}:{provider}:{episode_key}',
            tmdb_id=int(tmdb_id),
            title=title,
            media_type='tv',
            season=int(season or 1),
            episode=int(episode_key.split('E')[1]),
            episode_key=episode_key,
            cloud_name=provider,
            share_url=chosen_url,
            source_type='watchlist_scout',
            source_channel_id=str(chosen_source_channel or ''),
            source_message_id=chosen_source_msg,
        )
        db.add(new_resource)
        await db.flush()
        task = await TransferQueueService.enqueue(
            db, resource_id=new_resource.id, provider=provider,
            payload={'resource_id': new_resource.id, 'share_url': chosen_url,
                     'episode_keys': [episode_key], 'tmdb_id': int(tmdb_id), 'title': title,
                     'season': int(season or 1), 'source_type': 'watchlist_scout',
                     'source_channel_id': str(chosen_source_channel or ''), 'source_message_id': chosen_source_msg or 0},
            episode_keys=[episode_key],
        )
        pick_candidate = pick.get('candidate')
        if pick_candidate is not None:
            from app.transfer.candidate_service import mark_candidate_used

            pick_candidate.resource_id = new_resource.id
            pick_candidate.queue_task_id = task.id
            await mark_candidate_used(db, candidate=pick_candidate, queue_task_id=task.id)
        new_tasks.append({'episode_key': episode_key, 'task_id': task.id, 'status': task.status, 'reused': False})

    return {
        'tmdb_id': int(tmdb_id),
        'title': title,
        'season': int(season or 1),
        'switched_from_task': task_id,
        'new_tasks': new_tasks,
    }


async def _rescout_and_persist(
    db: AsyncSession,
    *,
    tmdb_id: int,
    title: str,
    season: int,
    episode_keys: list[str],
    resource_db_path: str,
    current_share_hash: str | None,
    source_msg: int | None,
    chosen_by_episode: dict[str, dict],
) -> None:
    """Ledger-based re-scout: search message store, persist every new
    candidate, then keep the best one that is not the current failure."""
    from app.scout.candidate_selector import evaluate

    search = MessageSearch(resource_db_path)
    for episode_key in episode_keys:
        found = search.search(title, episode_key)
        persisted: list = []
        for candidate in found:
            urls = list(getattr(candidate, 'urls', None) or [])
            if not urls:
                continue
            accepted, matched_url = evaluate(candidate, title=title, episode_key=episode_key)
            if not accepted or not matched_url:
                continue
            row = await record_candidate(
                db,
                tmdb_id=tmdb_id,
                title=title,
                season=season,
                episode_key=episode_key,
                provider=next((p for p in ('guangya', 'quark', 'baidu', '115', 'aliyun')
                               if p in matched_url.lower()), 'guangya'),
                share_url=matched_url,
                source_type='watchlist_scout',
                source_channel_id=str(getattr(candidate, 'chat_id', '')),
                source_message_id=int(getattr(candidate, 'message_id', 0) or 0),
            )
            persisted.append(row)
        for row in persisted:
            if current_share_hash and row.share_hash == current_share_hash:
                continue
            if source_msg and row.source_message_id == source_msg:
                continue
            if row.status in ('INVALID_SHARE', 'NO_VIDEO', 'EPISODE_MISMATCH'):
                continue
            selected = await select_alternative_candidate(
                db,
                tmdb_id=tmdb_id,
                season=season,
                episode_key=episode_key,
                exclude_share_hash=current_share_hash,
                exclude_source_message_id=source_msg,
            )
            if selected is not None:
                chosen_by_episode[episode_key] = {
                    'candidate': selected,
                    'share_url': selected.share_url,
                    'source_channel_id': selected.source_channel_id,
                    'source_message_id': selected.source_message_id,
                    'source': 'SCOUT',
                }
                break
