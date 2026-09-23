from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ChannelIngestMessage(Base):
    __tablename__ = 'channel_ingest_messages'
    __table_args__ = (UniqueConstraint('channel_id', 'message_id', name='uq_ingest_message_source'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    channel_id: Mapped[str] = mapped_column(String(128), nullable=False)
    message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    is_forward: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default='QUEUED', nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)


class ChannelIngestJob(Base):
    __tablename__ = 'channel_ingest_jobs'
    __table_args__ = (
        UniqueConstraint('channel_id', 'message_id', 'share_hash', name='uq_ingest_job_source'),
        Index('ix_ingest_job_status', 'status', 'created_at'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    channel_id: Mapped[str] = mapped_column(String(128), nullable=False)
    message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    is_forward: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    share_url: Mapped[str | None] = mapped_column(Text)
    share_hash: Mapped[str | None] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32), default='QUEUED', nullable=False)
    parsed_data: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    media_type: Mapped[str | None] = mapped_column(String(32))
    tmdb_id: Mapped[int | None] = mapped_column(Integer)
    title: Mapped[str | None] = mapped_column(String(512))
    year: Mapped[int | None] = mapped_column(Integer)
    season: Mapped[int | None] = mapped_column(Integer)
    detected_episodes: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    identity_status: Mapped[str | None] = mapped_column(String(32))
    ready_for_transfer: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    transfer_status: Mapped[str | None] = mapped_column(String(32))
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
