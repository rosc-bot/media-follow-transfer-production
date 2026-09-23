from datetime import datetime
from enum import StrEnum

from sqlalchemy import JSON, DateTime, Index, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ResourceStatus(StrEnum):
    """Lifecycle of a discovered candidate, independent of queue task state."""

    READY = "READY"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"
    INVALID = "INVALID"
    EXPIRED = "EXPIRED"


NON_BLOCKING_RESOURCE_STATUSES = frozenset({
    ResourceStatus.FAILED,
    ResourceStatus.REJECTED,
    ResourceStatus.INVALID,
    ResourceStatus.EXPIRED,
})

_NON_BLOCKING_SQL = "status IN ('FAILED', 'REJECTED', 'INVALID', 'EXPIRED')"


class Resource(Base):
    __tablename__ = 'resources'
    __table_args__ = (
        # Historical invalid/failed candidates remain auditable but cannot own a
        # future valid candidate identity. Unknown status values stay blocking.
        Index(
            'uq_resource_live_identity_key',
            'identity_key',
            unique=True,
            postgresql_where=text(f'NOT ({_NON_BLOCKING_SQL})'),
            sqlite_where=text(f'NOT ({_NON_BLOCKING_SQL})'),
        ),
        Index('ix_resource_episode', 'tmdb_id', 'season', 'episode'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    identity_key: Mapped[str] = mapped_column(String(512), nullable=False)
    tmdb_id: Mapped[int | None] = mapped_column(Integer)
    title: Mapped[str | None] = mapped_column(String(512))
    media_type: Mapped[str] = mapped_column(String(32), default='tv', nullable=False)
    year: Mapped[int | None] = mapped_column(Integer)
    season: Mapped[int | None] = mapped_column(Integer)
    episode: Mapped[int | None] = mapped_column(Integer)
    episode_key: Mapped[str | None] = mapped_column(String(64))
    version_key: Mapped[str | None] = mapped_column(String(128))
    cloud_name: Mapped[str | None] = mapped_column(String(64))
    share_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_channel_id: Mapped[str | None] = mapped_column(String(128))
    source_message_id: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32), default=ResourceStatus.READY, nullable=False)
    file_names: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    transferred_folder_id: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
