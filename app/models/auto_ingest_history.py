"""AutoIngestHistory ORM model — audit log of automated resource ingestions.

Migrated from SQLite schema:
    CREATE TABLE auto_ingest_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        season INTEGER NOT NULL,
        episodes TEXT,          -- JSON array
        share_url TEXT,
        provider TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(title, season, share_url)
    )
"""

from datetime import datetime

from sqlalchemy import JSON, DateTime, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class AutoIngestHistory(Base):
    __tablename__ = "auto_ingest_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    season: Mapped[int] = mapped_column(Integer, nullable=False)
    episodes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    share_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    provider: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("title", "season", "share_url", name="uq_auto_ingest_title_season_url"),
    )

    def __repr__(self) -> str:
        return (
            f"<AutoIngestHistory {self.title!r} S{self.season:02d} "
            f"eps={self.episodes} provider={self.provider!r}>"
        )
