"""Source-channel allowlist for the Scout resource index.

The authoritative whitelist is the ``channel_settings`` table (PostgreSQL):
only enabled channels whose role is RESOURCE or MANUAL_INGEST are allowed to
feed the resource index (via live ingest or historical backfill). Ordinary
chat groups and the future PUBLISH_ONLY role are never eligible, so a
publishing channel can not create a self-loop into resource discovery.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import SCOUT_ALLOWED_CHANNEL_ROLES
from app.models.channel import ChannelSetting


async def load_scout_allowed_channel_ids(db: AsyncSession) -> list[str]:
    """Return channel_ids allowed to feed the Scout resource index."""
    rows = (await db.scalars(
        select(ChannelSetting.channel_id).where(
            ChannelSetting.enabled.is_(True),
            ChannelSetting.role.in_(SCOUT_ALLOWED_CHANNEL_ROLES),
        )
    )).all()
    return [str(value) for value in rows]


def is_channel_scout_allowed(setting: object | None) -> bool:
    """True when a ChannelSetting is allowed to feed the Scout index."""
    if setting is None:
        return False
    if getattr(setting, 'enabled', True) is False:
        return False
    role = getattr(setting, 'role', None)
    return role in SCOUT_ALLOWED_CHANNEL_ROLES
