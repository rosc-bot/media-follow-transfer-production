"""FailedScoutPush ORM model — records failed resource-push attempts for retry/audit.

Migrated from SQLite schema:
    CREATE TABLE failed_scout_pushes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        season INTEGER NOT NULL DEFAULT 1,
        episodes TEXT,           -- JSON array
        share_url TEXT NOT NULL,
        provider TEXT,
        text_context TEXT,
        error_message TEXT,
        status TEXT DEFAULT 'FAILED',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

from datetime import datetime

from sqlalchemy import JSON, DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class FailedScoutPush(Base):
    __tablename__ = "failed_scout_pushes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    season: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    episodes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    share_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    provider: Mapped[str | None] = mapped_column(String(255), nullable=True)
    text_context: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="FAILED")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"<FailedScoutPush {self.title!r} S{self.season:02d} "
            f"status={self.status!r} url={self.share_url!r}>"
        )
