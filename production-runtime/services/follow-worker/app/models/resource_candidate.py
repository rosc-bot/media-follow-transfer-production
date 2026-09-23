"""Candidate resource history — one row per discovered share for an episode.

Records "which candidates were ever found (usable or not) for a
tmdb_id + season + episode" so automatic switch-resource can exclude
permanently-dead candidates while still retrying temporary failures.
Deliberately NOT a second full resource model — it is a candidate ledger.
"""

from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ResourceCandidateStatus(StrEnum):
    """Unified candidate state (Phase 2C §四).

    Permanent淘汰 (auto switch must exclude, never re-select):
        INVALID_SHARE / NO_VIDEO / EPISODE_MISMATCH / TRANSFERRED
    Temporary故障 (retry current resource is allowed later):
        TEMPORARY_FAILED / DISCOVERED / VALIDATED / SELECTED
    Auth问题 (the ACCOUNT is broken, not the resource):
        AUTH_BLOCKED
    """

    DISCOVERED = 'DISCOVERED'
    VALIDATED = 'VALIDATED'
    SELECTED = 'SELECTED'
    INVALID_SHARE = 'INVALID_SHARE'
    NO_VIDEO = 'NO_VIDEO'
    EPISODE_MISMATCH = 'EPISODE_MISMATCH'
    TEMPORARY_FAILED = 'TEMPORARY_FAILED'
    AUTH_BLOCKED = 'AUTH_BLOCKED'
    TRANSFERRED = 'TRANSFERRED'


#: Candidates that must never be re-selected by switch-resource.
PERMANENT_CANDIDATE_FAILURES = frozenset({
    ResourceCandidateStatus.INVALID_SHARE,
    ResourceCandidateStatus.NO_VIDEO,
    ResourceCandidateStatus.EPISODE_MISMATCH,
})

#: Statuses that are still eligible for future selection (or retry).
LIVE_CANDIDATE_STATUSES = frozenset({
    ResourceCandidateStatus.DISCOVERED,
    ResourceCandidateStatus.VALIDATED,
    ResourceCandidateStatus.SELECTED,
    ResourceCandidateStatus.TEMPORARY_FAILED,
})


class ResourceCandidate(Base):
    __tablename__ = 'resource_candidates'
    __table_args__ = (
        # Dedup key: tmdb/season/episode + stable share hash. source_message_id
        # is deliberately NOT unique — different channels forward the same link.
        UniqueConstraint('tmdb_id', 'season', 'episode_key', 'share_hash', name='uq_candidate_episode_hash'),
        Index('ix_candidate_episode_status', 'tmdb_id', 'season', 'episode_key', 'status'),
        Index('ix_candidate_hash', 'share_hash'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tmdb_id: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    year: Mapped[int | None] = mapped_column(Integer)
    season: Mapped[int] = mapped_column(Integer, nullable=False)
    episode_key: Mapped[str] = mapped_column(String(64), nullable=False)

    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    share_url: Mapped[str] = mapped_column(Text, nullable=False)
    share_hash: Mapped[str] = mapped_column(String(128), nullable=False)

    source_type: Mapped[str | None] = mapped_column(String(32))
    source_channel_id: Mapped[str | None] = mapped_column(String(128))
    source_message_id: Mapped[int | None] = mapped_column(Integer)

    resource_id: Mapped[int | None] = mapped_column(Integer)
    queue_task_id: Mapped[int | None] = mapped_column(Integer)

    status: Mapped[str] = mapped_column(String(32), default=ResourceCandidateStatus.DISCOVERED, nullable=False)
    failure_category: Mapped[str | None] = mapped_column(String(64))
    failure_reason: Mapped[str | None] = mapped_column(Text)

    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
