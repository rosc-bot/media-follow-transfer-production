"""BotSettings ORM model — key-value store for bot configuration.

Migrated from SQLite schema:
    CREATE TABLE bot_settings (key TEXT PRIMARY KEY, val TEXT, updated_at TIMESTAMP)

Default rows seeded on first access:
    auto_ingest_enabled  = '1'
    auto_ingest_categories = 'domestic,anime,western,jp-kr,movie'
    auto_ingest_mode     = 'LATEST'
    global_pause         = '0'
    follow_paused        = '1'
    transfer_paused      = '1'
"""

from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class BotSettings(Base):
    __tablename__ = "bot_settings"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    val: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    # --------------- convenience defaults ---------------
    DEFAULTS: dict[str, str] = {
        "auto_ingest_enabled": "1",
        "auto_ingest_categories": "domestic,anime,western,jp-kr,movie",
        "auto_ingest_mode": "LATEST",
        "global_pause": "0",
        "follow_paused": "1",
        "transfer_paused": "1",
    }

    def __repr__(self) -> str:
        return f"<BotSettings key={self.key!r} val={self.val!r}>"
