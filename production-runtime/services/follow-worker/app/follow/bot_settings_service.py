"""BotSettingsService — async key-value config store with typed convenience methods.

All methods are ``@staticmethod async`` accepting an ``AsyncSession`` as first arg,
matching the project convention established by ``SeriesWatchlistService``.

Seed defaults are defined on ``BotSettings.DEFAULTS`` and applied lazily on first
``get()`` when a key has no row yet.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bot_settings import BotSettings


class BotSettingsService:
    """Thin async service over the ``bot_settings`` key-value table."""

    # ------------------------------------------------------------------ #
    # Core CRUD
    # ------------------------------------------------------------------ #

    @staticmethod
    async def get(db: AsyncSession, key: str, default: str | None = None) -> str | None:
        """Return the value for *key*, falling back to ``BotSettings.DEFAULTS``
        then to *default*.  A missing key with a known default is **not** auto-
        persisted — call ``set()`` explicitly if persistence is desired."""
        row = (await db.execute(select(BotSettings).where(BotSettings.key == key))).scalar_one_or_none()
        if row is not None:
            return row.val
        return BotSettings.DEFAULTS.get(key, default)

    @staticmethod
    async def set(db: AsyncSession, key: str, val: str) -> None:
        """Upsert *key* = *val*.  Commits are left to the caller / session middleware."""
        row = (await db.execute(select(BotSettings).where(BotSettings.key == key))).scalar_one_or_none()
        if row is not None:
            row.val = val
        else:
            db.add(BotSettings(key=key, val=val))
        await db.flush()

    # ------------------------------------------------------------------ #
    # Auto-ingest helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    async def is_auto_ingest_enabled(db: AsyncSession) -> bool:
        val = await BotSettingsService.get(db, "auto_ingest_enabled", "1")
        return val == "1"

    @staticmethod
    async def toggle_auto_ingest_enabled(db: AsyncSession) -> bool:
        """Toggle and return the **new** state."""
        currently = await BotSettingsService.is_auto_ingest_enabled(db)
        new_val = "0" if currently else "1"
        await BotSettingsService.set(db, "auto_ingest_enabled", new_val)
        return new_val == "1"

    @staticmethod
    async def get_auto_ingest_categories(db: AsyncSession) -> list[str]:
        raw = await BotSettingsService.get(db, "auto_ingest_categories", "")
        if not raw:
            return []
        return [c.strip() for c in raw.split(",") if c.strip()]

    @staticmethod
    async def toggle_auto_ingest_category(db: AsyncSession, cat_key: str) -> list[str]:
        """Add or remove *cat_key* and return the **new** full list."""
        cats = await BotSettingsService.get_auto_ingest_categories(db)
        if cat_key in cats:
            cats.remove(cat_key)
        else:
            cats.append(cat_key)
        await BotSettingsService.set(db, "auto_ingest_categories", ",".join(cats))
        return cats

    # ------------------------------------------------------------------ #
    # Global pause
    # ------------------------------------------------------------------ #

    @staticmethod
    async def is_global_paused(db: AsyncSession) -> bool:
        val = await BotSettingsService.get(db, "global_pause", "0")
        return val == "1"

    @staticmethod
    async def toggle_global_pause(db: AsyncSession) -> bool:
        """Toggle the legacy global compatibility gate and return its new state.

        New callers should prefer ``set_follow_paused`` and ``set_transfer_paused``.
        A true global gate always wins over either independent setting.
        """
        currently = await BotSettingsService.is_global_paused(db)
        new_val = "0" if currently else "1"
        await BotSettingsService.set(db, "global_pause", new_val)
        return new_val == "1"

    @staticmethod
    async def is_follow_paused(db: AsyncSession) -> bool:
        """Return the effective follow pause state; legacy global pause wins."""
        if await BotSettingsService.is_global_paused(db):
            return True
        return (await BotSettingsService.get(db, "follow_paused", "1")) == "1"

    @staticmethod
    async def is_transfer_paused(db: AsyncSession) -> bool:
        """Return the effective transfer pause state; legacy global pause wins."""
        if await BotSettingsService.is_global_paused(db):
            return True
        return (await BotSettingsService.get(db, "transfer_paused", "1")) == "1"

    @staticmethod
    async def set_follow_paused(db: AsyncSession, paused: bool) -> None:
        await BotSettingsService.set(db, "follow_paused", "1" if paused else "0")

    @staticmethod
    async def set_transfer_paused(db: AsyncSession, paused: bool) -> None:
        await BotSettingsService.set(db, "transfer_paused", "1" if paused else "0")
