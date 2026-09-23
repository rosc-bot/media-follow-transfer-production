from datetime import datetime

from sqlalchemy import JSON, BigInteger, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class SeriesWatchlist(Base):
    __tablename__ = 'series_watchlist'
    __table_args__ = (
        UniqueConstraint('tmdb_id', 'season', 'subscriber_tg_id', name='uq_watchlist_series_subscriber'),
        Index('ix_watchlist_status_updated', 'status', 'updated_at'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tmdb_id: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    year: Mapped[int | None] = mapped_column(Integer)
    media_type: Mapped[str] = mapped_column(String(32), default='tv', nullable=False)
    season: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default='FOLLOWING', nullable=False)
    follow_mode: Mapped[str] = mapped_column(String(32), default='AUTO', nullable=False)
    total_episodes: Mapped[int | None] = mapped_column(Integer)
    last_aired_episode: Mapped[int | None] = mapped_column(Integer)
    tmdb_series_status: Mapped[str | None] = mapped_column(String(64))
    collected_episodes: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    remote_series_folder_id: Mapped[str | None] = mapped_column(String(256))
    remote_destination_kind: Mapped[str | None] = mapped_column(String(32))
    poster_path: Mapped[str | None] = mapped_column(String(1024))
    source: Mapped[str | None] = mapped_column(String(128))
    subscriber_tg_id: Mapped[int | None] = mapped_column(BigInteger)
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
