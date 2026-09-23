"""Candidate resource ledger service (Phase 2C §五/§六/§七).

Idempotent write/query helpers over ``resource_candidates``:

* ``record_candidate``  — discover/update one share for an episode (upsert on
  tmdb/season/episode_key/share_hash).  Re-running a follow cycle never
  duplicates a candidate.
* ``mark_candidate_failure`` — apply a unified status from a transfer error
  category (permanent vs temporary vs auth), never inventing per-module
  strings.
* ``select_alternative_candidate`` — pick the next usable candidate for
  switch-resource, excluding permanently-dead ones and the current share.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.resource_candidate import (
    LIVE_CANDIDATE_STATUSES,
    PERMANENT_CANDIDATE_FAILURES,
    ResourceCandidate,
    ResourceCandidateStatus,
)
from app.transfer.normalization import share_hash

logger = logging.getLogger(__name__)

#: Transfer error category -> candidate status (Phase 2C §四/§七).
#: Permanent → never re-select; temporary → retryable; auth → account, not
#: the resource, so the candidate is never blacklisted.
CATEGORY_TO_CANDIDATE_STATUS: dict[str, ResourceCandidateStatus] = {
    'INVALID_SHARE': ResourceCandidateStatus.INVALID_SHARE,
    'SHARE_NOT_FOUND': ResourceCandidateStatus.INVALID_SHARE,
    'EMPTY_SHARE': ResourceCandidateStatus.NO_VIDEO,
    'NO_VIDEO_FILES': ResourceCandidateStatus.NO_VIDEO,
    'EPISODE_MISMATCH': ResourceCandidateStatus.EPISODE_MISMATCH,
    # temporary / transport problems
    'NETWORK_TIMEOUT': ResourceCandidateStatus.TEMPORARY_FAILED,
    'NETWORK_ERROR': ResourceCandidateStatus.TEMPORARY_FAILED,
    'RATE_LIMITED': ResourceCandidateStatus.TEMPORARY_FAILED,
    'REMOTE_5XX': ResourceCandidateStatus.TEMPORARY_FAILED,
    'READBACK_UNVERIFIED': ResourceCandidateStatus.TEMPORARY_FAILED,
    # auth — keep the candidate live, block the account problem
    'AUTH_EXPIRED': ResourceCandidateStatus.AUTH_BLOCKED,
    'AUTH_INVALID': ResourceCandidateStatus.AUTH_BLOCKED,
}


def stable_share_key(url: str) -> str:
    """Stable dedup key: sha256 of the normalized URL."""
    return share_hash(url)


async def record_candidate(
    db: AsyncSession,
    *,
    tmdb_id: int,
    title: str,
    season: int,
    episode_key: str,
    provider: str,
    share_url: str,
    source_type: str | None = None,
    source_channel_id: str | None = None,
    source_message_id: int | None = None,
    resource_id: int | None = None,
    queue_task_id: int | None = None,
    year: int | None = None,
    status: ResourceCandidateStatus = ResourceCandidateStatus.DISCOVERED,
) -> ResourceCandidate:
    """Idempotent upsert of one discovered candidate."""
    key = stable_share_key(share_url)
    existing = await db.scalar(
        select(ResourceCandidate).where(
            ResourceCandidate.tmdb_id == tmdb_id,
            ResourceCandidate.season == season,
            ResourceCandidate.episode_key == episode_key,
            ResourceCandidate.share_hash == key,
        )
    )
    now = datetime.now(UTC)
    if existing is not None:
        existing.last_checked_at = now
        existing.title = title
        if resource_id is not None:
            existing.resource_id = resource_id
        if queue_task_id is not None:
            existing.queue_task_id = queue_task_id
        return existing
    candidate = ResourceCandidate(
        tmdb_id=tmdb_id,
        title=title,
        year=year,
        season=season,
        episode_key=episode_key,
        provider=provider,
        share_url=share_url,
        share_hash=key,
        source_type=source_type,
        source_channel_id=source_channel_id,
        source_message_id=source_message_id,
        resource_id=resource_id,
        queue_task_id=queue_task_id,
        status=status,
        discovered_at=now,
        last_checked_at=now,
        attempt_count=0,
    )
    db.add(candidate)
    await db.flush()
    return candidate


async def get_candidates_for_episode(
    db: AsyncSession,
    *,
    tmdb_id: int,
    season: int,
    episode_key: str,
    exclude_statuses: frozenset[ResourceCandidateStatus] | None = None,
) -> list[ResourceCandidate]:
    """All candidates for one episode, optionally excluding dead ones."""
    stmt = (
        select(ResourceCandidate)
        .where(
            ResourceCandidate.tmdb_id == tmdb_id,
            ResourceCandidate.season == season,
            ResourceCandidate.episode_key == episode_key,
        )
        .order_by(ResourceCandidate.discovered_at.asc())
    )
    if exclude_statuses:
        exclude_values = [str(s) for s in exclude_statuses]
        stmt = stmt.where(ResourceCandidate.status.notin_(exclude_values))
    return list((await db.execute(stmt)).scalars())


async def select_alternative_candidate(
    db: AsyncSession,
    *,
    tmdb_id: int,
    season: int,
    episode_key: str,
    exclude_share_hash: str | None = None,
    exclude_source_message_id: int | None = None,
) -> ResourceCandidate | None:
    """Best next candidate for switch-resource.

    * permanently-dead candidates (INVALID_SHARE/NO_VIDEO/EPISODE_MISMATCH)
      are never returned;
    * VALIDATED is preferred over DISCOVERED (Phase 2C §六 5);
    * the current share hash / source message is excluded so A→A is impossible.
    """
    candidates = await get_candidates_for_episode(
        db,
        tmdb_id=tmdb_id,
        season=season,
        episode_key=episode_key,
        exclude_statuses=PERMANENT_CANDIDATE_FAILURES,
    )
    usable = [
        c for c in candidates
        if not (exclude_share_hash and c.share_hash == exclude_share_hash)
        and not (exclude_source_message_id and c.source_message_id == exclude_source_message_id)
        and c.status in LIVE_CANDIDATE_STATUSES
    ]
    if not usable:
        return None
    return min(
        usable,
        key=lambda c: (0 if c.status == ResourceCandidateStatus.VALIDATED else 1, c.discovered_at),
    )


async def mark_candidate_failure(
    db: AsyncSession,
    *,
    candidate: ResourceCandidate | None,
    category: str | None,
    failure_reason: str | None = None,
) -> None:
    """Apply the unified candidate status for a transfer error category.

    AUTH categories never pollute the candidate as INVALID — the resource may
    be fine; only the account is broken (Phase 2C §四).
    """
    if candidate is None:
        return
    mapped = CATEGORY_TO_CANDIDATE_STATUS.get(category or '')
    if mapped is None:
        return
    candidate.status = mapped
    candidate.failure_category = category
    if failure_reason:
        candidate.failure_reason = failure_reason[:2000]
    candidate.last_checked_at = datetime.now(UTC)
    await db.flush()


async def mark_candidate_used(
    db: AsyncSession,
    *,
    candidate: ResourceCandidate | None,
    queue_task_id: int | None = None,
) -> None:
    """Mark the selected candidate as SELECTED + bump attempts."""
    if candidate is None:
        return
    candidate.status = ResourceCandidateStatus.SELECTED
    candidate.attempt_count = (candidate.attempt_count or 0) + 1
    candidate.last_checked_at = datetime.now(UTC)
    candidate.last_used_at = datetime.now(UTC)
    if queue_task_id is not None:
        candidate.queue_task_id = queue_task_id
    await db.flush()


async def mark_candidate_transferred(
    db: AsyncSession,
    *,
    candidate: ResourceCandidate | None,
) -> None:
    """Permanent success — the episode is collected via this candidate."""
    if candidate is None:
        return
    candidate.status = ResourceCandidateStatus.TRANSFERRED
    candidate.last_checked_at = datetime.now(UTC)
    candidate.last_used_at = datetime.now(UTC)
    await db.flush()
