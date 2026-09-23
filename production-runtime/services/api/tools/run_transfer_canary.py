"""Single-task Canary transfer executor (Phase 2C §十–§十六).

A deliberately narrow, admin-only tool that exercises the REAL production
transfer chain (TransferOrchestrator → GuangyaAdapter → restore → readback)
for exactly ONE task, without ever unlocking the ordinary Transfer Worker.

Gates (both required for any real cloud write):
  1. command line:  --execute  AND  --confirm-task-id N == --task-id N
  2. process env:   CANARY_CLOUD_WRITE_ENABLED=true  (never in long-lived .env)

Default mode is --dry-run: every preflight check runs read-only and the task
state is NEVER changed.  No all/latest/range/wildcard task selection exists.

Usage:
  python -m tools.run_transfer_canary --task-id 123                # dry-run
  python -m tools.run_transfer_canary --list-safe                 # read-only report
  CANARY_CLOUD_WRITE_ENABLED=true python -m tools.run_transfer_canary \
      --task-id 123 --execute --confirm-task-id 123               # real canary
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.follow.episode_keys import canonical_episode_key
from app.models.cloud import CloudConfig, CloudDiskInventory
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.transfer.status import SUCCESS_TERMINAL_STATUSES, TransferStatus
from app.transfer.task_identity import task_matches_episode

CANARY_SAFE = 'CANARY_SAFE'
CANARY_REJECTED = 'CANARY_REJECTED'
NEEDS_REVIEW = 'NEEDS_REVIEW'

#: Task statuses the canary may even consider executable.
_EXECUTABLE_TASK_STATUSES = frozenset({TransferStatus.QUEUED, TransferStatus.RETRY_WAIT})
#: Task classifications that must never be canary'd.
_BLOCKED_TASK_MARKERS = frozenset({'AUTH_BLOCKED', 'LEGACY_BROKEN_TASK', 'NEEDS_REVIEW'})
_ALLOWED_CANARY_SOURCE_TYPES = frozenset({'watchlist_scout', 'framehdr'})


async def validate_remote_canary(*, provider: str, share_url: str, auth_ref: str, target_folder_id: str, episode_keys: list[str], season: int | None = None) -> dict:
    """Compatibility export for the Phase 2E explicit, read-only remote checks."""
    from app.transfer.canary_preflight import validate_remote_canary as _validate

    return await _validate(
        provider=provider,
        share_url=share_url,
        auth_ref=auth_ref,
        target_folder_id=target_folder_id,
        episode_keys=episode_keys,
        season=season,
    )


class CanaryRejected(Exception):
    """A preflight check failed; the cloud disk must not be written."""


async def _preflight(
    db: AsyncSession,
    task_id: int,
    *,
    remote_validator: Callable[..., Awaitable[dict]] | None = None,
) -> dict:
    """Read-only preflight for one task; raises CanaryRejected on failure.

    Returns the hydrated report with CANARY_SAFE / CANARY_REJECTED /
    NEEDS_REVIEW classification.  Never mutates the task.
    """
    checks: list[dict] = []
    report: dict = {
        'task_id': task_id,
        'verdict': CANARY_SAFE,
        'checks': checks,
        'rejections': [],
    }

    def ok(name: str, detail: str = '') -> None:
        checks.append({'check': name, 'ok': True, 'detail': detail})

    def reject(name: str, detail: str) -> None:
        checks.append({'check': name, 'ok': False, 'detail': detail})
        report['rejections'].append({'check': name, 'detail': detail})
        report['verdict'] = CANARY_REJECTED

    def review(name: str, detail: str) -> None:
        checks.append({'check': name, 'ok': False, 'detail': detail})
        if report['verdict'] == CANARY_SAFE:
            report['verdict'] = NEEDS_REVIEW

    # 1. Task exists & executable status
    task = await db.get(TransferQueueTask, task_id)
    if task is None:
        reject('task_exists', f'task #{task_id} does not exist')
        raise CanaryRejected('task does not exist')
    if task.status in _BLOCKED_TASK_MARKERS:
        reject('task_not_blocked', f'task status is {task.status}')
    elif task.status not in _EXECUTABLE_TASK_STATUSES:
        review('task_executable', f'task status is {task.status} (need QUEUED/RETRY_WAIT)')
    else:
        ok('task_executable', f'status={task.status}')
    if task.locked_at is not None:
        review('task_not_locked', f'locked_at={task.locked_at} by {task.locked_by}')

    payload = dict(task.payload or {})
    resource = await db.get(Resource, task.resource_id) if task.resource_id else None

    # 2. Resource exists
    if task.resource_id is None:
        reject('resource_exists', 'task has no resource_id')
    elif resource is None:
        reject('resource_exists', f'resource #{task.resource_id} missing')
    else:
        ok('resource_exists', f'resource #{resource.id}')

    tmdb_id = payload.get('tmdb_id') or (resource.tmdb_id if resource else None)
    season = payload.get('season') or (resource.season if resource else None)
    episode_keys = list(payload.get('episode_keys')
                        or ([resource.episode_key] if resource and resource.episode_key else []))
    share_url = payload.get('share_url') or (resource.share_url if resource else None)

    # 3/4/5. identity present
    if not tmdb_id:
        reject('tmdb_id', 'tmdb_id missing')
    else:
        ok('tmdb_id', str(tmdb_id))
    if not season:
        reject('season', 'season missing')
    else:
        ok('season', str(season))
    if not episode_keys:
        reject('episode', 'episode_keys missing')
    else:
        ok('episode', ','.join(map(str, episode_keys)))

    # 6. watchlist identity is exact TMDB + season + title; not merely a title guess.
    watchlist = None
    if tmdb_id and season:
        watchlists = (await db.execute(select(SeriesWatchlist).where(
            SeriesWatchlist.tmdb_id == int(tmdb_id),
            SeriesWatchlist.season == int(season),
        ))).scalars().all()
        title_matches = [row for row in watchlists if resource is not None and row.title == resource.title]
        if len(title_matches) != 1:
            reject('watchlist_identity', f'exact TMDB/season/title match count={len(title_matches)}')
        else:
            watchlist = title_matches[0]
            ok('watchlist_identity', f'watchlist #{watchlist.id}')
        collected = {
            key
            for value in (watchlist.collected_episodes if watchlist else []) or []
            if (key := canonical_episode_key(int(season), value)) is not None
        }
        already = [
            ep for ep in map(str, episode_keys)
            if (canonical := canonical_episode_key(int(season), ep)) is not None
            and canonical in collected
        ]
        if already:
            reject('not_collected', f'already collected: {already}')
        else:
            ok('not_collected', 'none of the episode keys are collected')

    # 7. not already in cloud inventory
    if tmdb_id and season:
        inv = (await db.execute(select(CloudDiskInventory).where(
            CloudDiskInventory.tmdb_id == int(tmdb_id),
            CloudDiskInventory.season == int(season),
        ))).scalars().all()
        inv_eps = {int(i.episode) for i in inv}
        target_eps = {int(ep.split('E')[-1]) for ep in episode_keys if 'E' in str(ep)}
        overlap = sorted(inv_eps & target_eps)
        if overlap:
            reject('not_in_cloud_inventory', f'cloud inventory already has episodes: {overlap}')
        else:
            ok('not_in_cloud_inventory', 'no inventory overlap')

    # 8. no other SUCCESS/COMPLETED task already completed the same episodes
    if tmdb_id:
        dup_tasks = (await db.execute(select(TransferQueueTask).where(
            TransferQueueTask.status.in_(SUCCESS_TERMINAL_STATUSES),
        ))).scalars().all()
        resource_ids = {row.resource_id for row in dup_tasks if row.resource_id is not None}
        resources = {
            row.id: row
            for row in (await db.scalars(select(Resource).where(Resource.id.in_(resource_ids)))).all()
        } if resource_ids else {}
        dup_hits = [
            row.id
            for row in dup_tasks
            if task_matches_episode(
                row.payload,
                tmdb_id=int(tmdb_id),
                season=int(season or 1),
                episode_keys={str(key) for key in episode_keys},
                resource=resources.get(row.resource_id),
            )
        ]
        if dup_hits:
            reject('no_duplicate_success', f'episodes already done by tasks: {dup_hits}')
        else:
            ok('no_duplicate_success', 'no earlier SUCCESS task for these episodes')

    # 9. no other active task on the same episodes
    if tmdb_id:
        active = (await db.execute(select(TransferQueueTask).where(
            TransferQueueTask.status.in_([TransferStatus.QUEUED, TransferStatus.RUNNING, TransferStatus.RETRY_WAIT]),
        ))).scalars().all()
        active_hits = []
        for t in active:
            if t.id == task_id:
                continue
            t_eps = list((t.payload or {}).get('episode_keys') or [])
            if {str(e) for e in t_eps} & {str(e) for e in episode_keys} and (t.payload or {}).get('tmdb_id') == int(tmdb_id):
                active_hits.append(t.id)
        if active_hits:
            review('no_active_duplicate', f'other active tasks on same episodes: {active_hits}')
        else:
            ok('no_active_duplicate', 'no other active task for these episodes')

    # 10. share_url present
    if not share_url:
        reject('share_url', 'share_url missing in payload/resource')
    else:
        ok('share_url_present', f'{share_url[:45]}... (masked)')

    provider = str(payload.get('provider') or (resource.cloud_name if resource else '') or 'guangya').lower()
    report['provider'] = provider

    # 11. cloud config exists & enabled + credentials present
    cloud_cfg = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
    if cloud_cfg is None:
        reject('cloud_config', f'cloud provider {provider} not configured')
    elif not cloud_cfg.enabled:
        reject('cloud_config', f'cloud provider {provider} disabled in config')
    else:
        ok('cloud_config', f'{provider} enabled')
        report['target_folder_id'] = cloud_cfg.target_folder_id
        report['ongoing_target_folder_id'] = cloud_cfg.ongoing_target_folder_id
        if not (payload.get('auth_token') or cloud_cfg.auth_ref):
            reject('auth_credentials', f'no credentials for {provider}')
        else:
            ok('auth_credentials', 'auth_ref present')

    # 12. candidate is not permanently invalid; report only the candidate for this share.
    candidate_statuses: list[str] = []
    if task.resource_id and resource and tmdb_id is not None and season is not None:
        from app.transfer.candidate_service import get_candidates_for_episode, stable_share_key

        current_hash = stable_share_key(share_url) if share_url else None
        for episode_key in episode_keys:
            candidates = await get_candidates_for_episode(
                db, tmdb_id=int(tmdb_id), season=int(season), episode_key=str(episode_key),
            )
            current = [candidate for candidate in candidates if candidate.resource_id == resource.id or candidate.share_hash == current_hash]
            candidate_statuses.extend(str(candidate.status) for candidate in current)
            for candidate in current:
                if candidate.status in ('INVALID_SHARE', 'NO_VIDEO', 'EPISODE_MISMATCH'):
                    reject('candidate_not_invalid',
                           f'candidate #{candidate.id} permanently invalid ({candidate.status})')
                    break
    report['candidate_status'] = sorted(set(candidate_statuses)) or None

    # 13. source provenance must be an automatic Scout source, never an opaque legacy source.
    source_type = str(payload.get('source_type') or (resource.source_type if resource else '')).strip().lower()
    if source_type not in _ALLOWED_CANARY_SOURCE_TYPES:
        reject('source_type', f'unsupported source_type={source_type or "missing"}')
    else:
        ok('source_type', source_type)

    # 14. optional remote read-only validation.  CLI list-safe and execute provide it;
    # unit-level preflight without a validator remains a pure DB dry-run.
    if remote_validator is not None and cloud_cfg is not None and share_url and cloud_cfg.auth_ref:
        remote = await remote_validator(
            provider=provider,
            share_url=str(share_url),
            auth_ref=str(cloud_cfg.auth_ref),
            target_folder_id=str(cloud_cfg.target_folder_id or cloud_cfg.ongoing_target_folder_id or ''),
            episode_keys=[str(key) for key in episode_keys],
        )
        report['remote_validation'] = remote
        for key, check_name in (
            ('share_readable', 'share_readable'),
            ('has_video', 'share_has_video'),
            ('episode_match', 'episode_match'),
            ('auth_usable', 'guangya_auth_usable'),
        ):
            if remote.get(key):
                ok(check_name, str(remote.get('detail') or 'read-only validated'))
            else:
                reject(check_name, str(remote.get('detail') or 'read-only validation failed'))

    # 15. transfer pause gate (informational; canary honours it at execution)
    from app.follow.bot_settings_service import BotSettingsService
    paused = await BotSettingsService.is_transfer_paused(db)
    report['transfer_paused'] = paused
    ok('pause_gate', 'transfer_paused=1' if paused else 'transfer_paused=0')

    # 14. cloud write gate (informational in dry-run)
    from app.transfer.adapters import effective_cloud_write_enabled
    report['cloud_write_enabled'] = effective_cloud_write_enabled(permit_canary_env=False)
    report['canary_cloud_write_env'] = os.environ.get('CANARY_CLOUD_WRITE_ENABLED', '')

    # 15. destination determinable (static read from config roots)
    if cloud_cfg is not None and cloud_cfg.enabled:
        if not cloud_cfg.target_folder_id and not cloud_cfg.ongoing_target_folder_id:
            reject('destination_root', 'no target/ongoing root folder configured')
        else:
            ok('destination_root', 'static roots present')

    report['title'] = payload.get('title') or (resource.title if resource else None)
    report['episode_keys'] = episode_keys
    report['resource'] = resource.id if resource else None
    report['attempt_count'] = task.attempt_count
    report['reason'] = ' / '.join(r['check'] for r in report['rejections']) or 'all checks passed'
    return report


async def run_preflight(
    task_id: int,
    *,
    session_factory=AsyncSessionLocal,
    remote_validator: Callable[..., Awaitable[dict]] | None = None,
) -> dict:
    """Read-only Phase 2E preflight with an explicit check matrix."""
    from app.transfer.canary_preflight import preflight_task

    async with session_factory() as db:
        return await preflight_task(db, task_id, remote_validator=remote_validator)


async def list_safe_candidates(
    limit: int = 30,
    *,
    session_factory=AsyncSessionLocal,
    remote_validator: Callable[..., Awaitable[dict]] | None = validate_remote_canary,
) -> dict:
    """Read-only CANARY_SAFE report over QUEUED/RETRY_WAIT tasks."""
    rows = []
    async with session_factory() as db:
        tasks = (await db.execute(
            select(TransferQueueTask)
            .where(TransferQueueTask.status.in_([TransferStatus.QUEUED, TransferStatus.RETRY_WAIT]))
            .order_by(TransferQueueTask.id.asc())
            .limit(limit)
        )).scalars().all()
        for task in tasks:
            pre = await run_preflight(task.id, session_factory=session_factory, remote_validator=remote_validator)
            resource = await db.get(Resource, task.resource_id) if task.resource_id else None
            report = {
                'task_id': task.id,
                'title': pre.get('title'),
                'tmdb_id': pre.get('tmdb_id'),
                'season': pre.get('season'),
                'episode_key': pre.get('episode_key'),
                'episode_keys': pre.get('episode_key'),
                'provider': None,
                'share_url': 'present' if pre.get('checks', {}).get('check_share_url', {}).get('result') == 'PASS' else None,
                'resource_status': resource.status if resource else None,
                'candidate_status': pre.get('checks', {}).get('check_candidate_status'),
                'existing_collected': pre.get('checks', {}).get('check_collected'),
                'cloud_inventory': pre.get('checks', {}).get('check_cloud_inventory'),
                'duplicate_task': pre.get('checks', {}).get('check_duplicate_success'),
                'validation': pre,
                'verdict': pre['preflight_status'],
                'preflight_status': pre['preflight_status'],
                'execution_gate': pre['execution_gate'],
                'failure_code': pre.get('failure_code'),
                'failure_detail': pre.get('failure_detail'),
                'reason': pre.get('reason'),
            }
            rows.append(report)
    safe = [r for r in rows if r['verdict'] == CANARY_SAFE]
    rejected = [r for r in rows if r['verdict'] == CANARY_REJECTED]
    review = [r for r in rows if r['verdict'] == NEEDS_REVIEW]
    return {
        'total_checked': len(rows),
        'CANARY_SAFE': len(safe),
        'CANARY_REJECTED': len(rejected),
        'NEEDS_REVIEW': len(review),
        'recommended_task_ids': [r['task_id'] for r in safe[:3]],
        'rows': rows,
    }


async def run_execute(task_id: int, confirm_task_id: int) -> dict:
    """Execute ONE task through the real production transfer chain."""
    if confirm_task_id != task_id:
        raise SystemExit('refusing: --confirm-task-id must equal --task-id (mitigate mistyping)')
    if not os.environ.get('CANARY_CLOUD_WRITE_ENABLED', '').strip().lower() in ('1', 'true', 'yes'):
        raise SystemExit('refusing: CANARY_CLOUD_WRITE_ENABLED=true is required for real cloud writes (Phase 2C §十三)')
    # Preflight read-only; a rejection aborts BEFORE any write.
    preflight = await run_preflight(task_id, remote_validator=validate_remote_canary)
    if preflight['verdict'] == CANARY_REJECTED:
        return {'executed': False, 'preflight': preflight, 'reason': 'CANARY_REJECTED — no cloud write'}
    remote = preflight.get('remote_validation') or {}
    selection_snapshot = remote.get('selection_result') or {}
    selected_ids = list(selection_snapshot.get('selected_file_ids') or [])
    selection_decision = str(selection_snapshot.get('decision') or '')
    if not selected_ids or selection_decision not in {'EXACT_SINGLE_EPISODE', 'WHOLE_SHARE', 'COLLECTION'}:
        return {
            'executed': False,
            'preflight': preflight,
            'reason': 'CANARY_ABORTED_SELECTION_TOO_BROAD',
        }

    from app.transfer.adapters.guangya import GuangyaAdapter
    from app.transfer.orchestrator import TransferOrchestrator
    from app.transfer.queue_worker import TransferQueueWorker

    # Canary uses the SAME orchestrator and real adapter — only the task source
    # differs (explicit task_id instead of claim_next).
    adapter = GuangyaAdapter(write_enabled=True)
    orchestrator = TransferOrchestrator(adapters={
        'guangya': adapter,
        'mobile': TransferOrchestrator().adapters['mobile'],
        'alist': TransferOrchestrator().adapters['alist'],
        'dry-run': TransferOrchestrator().adapters['dry-run'],
    })
    worker = TransferQueueWorker(
        session_factory=AsyncSessionLocal,
        orchestrator=orchestrator,
        worker_id=f'canary:{os.getpid()}',
    )
    result = await worker.execute_single_task(
        task_id,
        canary_override_pause=True,
        selection_snapshot=selection_snapshot,
    )
    return {'executed': True, 'preflight': preflight, 'result': result}


def main() -> None:
    parser = argparse.ArgumentParser(description='Single-task transfer Canary (Phase 2C)')
    parser.add_argument('--task-id', type=int, help='exact single task id (no all/latest/range)')
    parser.add_argument('--confirm-task-id', type=int, default=None, help='must equal --task-id for real execution')
    parser.add_argument('--execute', action='store_true', help='real execution (dry-run default)')
    parser.add_argument('--list-safe', action='store_true', help='read-only CANARY_SAFE report')
    parser.add_argument('--json', action='store_true', help='raw JSON output')
    args = parser.parse_args()

    if args.list_safe:
        report = asyncio.run(list_safe_candidates())
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        sys.exit(0)
    if not args.task_id:
        parser.error('--task-id is required (exactly one task)')
    if args.task_id <= 0:
        parser.error('--task-id must be a positive integer')

    if args.execute:
        result = asyncio.run(run_execute(args.task_id, args.confirm_task_id or 0))
    else:
        result = asyncio.run(run_preflight(args.task_id, remote_validator=validate_remote_canary))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    sys.exit(0)


if __name__ == '__main__':
    main()
