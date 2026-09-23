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

from app.follow.episode_keys import canonical_episode_key
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
from app.transfer.status import EXECUTION_ACTIVE_STATUSES

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

    # Group episode-level candidate picks by the actual share identity before
    # creating Resources or queue rows. One share that covers several missing
    # episodes becomes one Resource and one MISSING_EPISODES task.
    active_tasks = (await db.execute(
        select(TransferQueueTask).where(TransferQueueTask.status.in_(EXECUTION_ACTIVE_STATUSES))
    )).scalars().all()
    groups: dict[tuple[str, str], dict] = {}
    for raw_episode_key, pick in chosen_by_episode.items():
        episode_key = canonical_episode_key(int(season or 1), raw_episode_key)
        chosen_url = str(pick['share_url'])
        provider = 'guangya'
        if 'quark' in chosen_url.casefold():
            provider = 'quark'
        elif 'baidu' in chosen_url.casefold():
            provider = 'baidu'
        elif '115' in chosen_url.casefold():
            provider = '115'
        elif 'alipan' in chosen_url.casefold() or 'aliyundrive' in chosen_url.casefold():
            provider = 'aliyun'
        group_key = (provider, stable_share_key(chosen_url))
        group = groups.setdefault(group_key, {
            'provider': provider,
            'share_url': chosen_url,
            'episode_keys': [],
            'picks': {},
            'source_channel_id': pick.get('source_channel_id') or source_channel or '',
            'source_message_id': pick.get('source_message_id') or int(source_msg or 0) or None,
        })
        if episode_key and episode_key not in group['episode_keys']:
            group['episode_keys'].append(episode_key)
            group['picks'][episode_key] = pick

    new_tasks: list[dict] = []
    for group in groups.values():
        provider = group['provider']
        chosen_url = group['share_url']
        group_episode_keys = sorted(
            group['episode_keys'],
            key=lambda value: (int(value[1:3]), int(value.split('E', 1)[1])),
        )
        if not group_episode_keys:
            continue
        active_by_episode: dict[str, TransferQueueTask] = {}
        for active_task in active_tasks:
            active_payload = dict(active_task.payload or {})
            active_url = str(active_payload.get('share_url') or '')
            try:
                same_identity = (
                    int(active_payload.get('tmdb_id') or 0) == int(tmdb_id)
                    and int(active_payload.get('season') or season or 1) == int(season or 1)
                    and bool(active_url)
                    and stable_share_key(active_url) == stable_share_key(chosen_url)
                )
            except (TypeError, ValueError):
                same_identity = False
            if not same_identity:
                continue
            active_keys = {
                key
                for value in (active_payload.get('episode_keys') or [])
                if (key := canonical_episode_key(int(season or 1), value)) is not None
            }
            for episode_key in group_episode_keys:
                if episode_key in active_keys:
                    active_by_episode.setdefault(episode_key, active_task)

        reused_by_task: dict[int, list[str]] = {}
        for episode_key, active_task in active_by_episode.items():
            reused_by_task.setdefault(active_task.id, []).append(episode_key)
        for active_task_id, reused_keys in reused_by_task.items():
            active_task = next(task for task in active_tasks if task.id == active_task_id)
            for episode_key in reused_keys:
                pick = group['picks'][episode_key]
                candidate = pick.get('candidate')
                if candidate is not None:
                    candidate.resource_id = active_task.resource_id
                    candidate.queue_task_id = active_task.id
                    from app.transfer.candidate_service import mark_candidate_used

                    await mark_candidate_used(db, candidate=candidate, queue_task_id=active_task.id)
            new_tasks.append({
                'episode_key': reused_keys[0],
                'episode_keys': sorted(reused_keys),
                'task_id': active_task.id,
                'status': active_task.status,
                'reused': True,
            })

        remaining_keys = [key for key in group_episode_keys if key not in active_by_episode]
        if not remaining_keys:
            continue
        chosen_source_channel = str(group['source_channel_id'] or '')
        chosen_source_msg = group['source_message_id']
        season_number = int(season or 1)
        identity_key = f'{int(tmdb_id)}:{provider}:S{season_number:02d}:{stable_share_key(chosen_url)}'
        new_resource = await db.scalar(select(Resource).where(Resource.identity_key == identity_key))
        if new_resource is None:
            first_episode = remaining_keys[0]
            new_resource = Resource(
                identity_key=identity_key,
                tmdb_id=int(tmdb_id),
                title=title,
                media_type='tv',
                season=season_number,
                episode=int(first_episode.split('E', 1)[1]),
                episode_key=first_episode,
                cloud_name=provider,
                share_url=chosen_url,
                source_type='watchlist_scout',
                source_channel_id=chosen_source_channel,
                source_message_id=chosen_source_msg,
            )
            db.add(new_resource)
            await db.flush()
        else:
            new_resource.status = 'READY'
            new_resource.title = title
            new_resource.share_url = chosen_url
            new_resource.source_channel_id = chosen_source_channel
            new_resource.source_message_id = chosen_source_msg

        task_payload = {
            'resource_id': new_resource.id,
            'share_url': chosen_url,
            'episode_keys': remaining_keys,
            'selected_episode_keys': remaining_keys,
            'selection_mode': 'MISSING_EPISODES',
            'tmdb_id': int(tmdb_id),
            'title': title,
            'season': season_number,
            'source_type': 'watchlist_scout',
            'source_channel_id': chosen_source_channel,
            'source_message_id': chosen_source_msg or 0,
        }
        task = await TransferQueueService.enqueue(
            db,
            resource_id=new_resource.id,
            provider=provider,
            payload=task_payload,
            episode_keys=remaining_keys,
        )
        for episode_key in remaining_keys:
            candidate = group['picks'][episode_key].get('candidate')
            if candidate is not None:
                candidate.resource_id = new_resource.id
                candidate.queue_task_id = task.id
                from app.transfer.candidate_service import mark_candidate_used

                await mark_candidate_used(db, candidate=candidate, queue_task_id=task.id)
        new_tasks.append({
            'episode_key': remaining_keys[0],
            'episode_keys': remaining_keys,
            'task_id': task.id,
            'status': task.status,
            'reused': False,
        })

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
