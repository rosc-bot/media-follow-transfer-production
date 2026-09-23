"""IgnoredMissing ORM model — tracks series/episodes excluded from missing-episode alerts.

Migrated from SQLite schema:
    CREATE TABLE ignored_missing (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        season INTEGER NOT NULL DEFAULT 1,
        episode INTEGER NOT NULL DEFAULT 0,   -- 0 = ignore entire season
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(title, season, episode)
    )
"""

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class IgnoredMissing(Base):
    __tablename__ = "ignored_missing"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    season: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    episode: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("title", "season", "episode", name="uq_ignored_missing_title_season_ep"),
    )

    def __repr__(self) -> str:
        ep = f"E{self.episode:02d}" if self.episode else "全季"
        return f"<IgnoredMissing {self.title!r} S{self.season:02d}{ep}>"
