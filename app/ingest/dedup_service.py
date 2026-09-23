from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.resource import NON_BLOCKING_RESOURCE_STATUSES, Resource


class DedupService:
    @staticmethod
    async def find_existing(db: AsyncSession, identity_key: str) -> Resource | None:
        """Return only a live resource that is allowed to block new candidates."""
        return await db.scalar(
            select(Resource).where(
                Resource.identity_key == identity_key,
                Resource.status.not_in(NON_BLOCKING_RESOURCE_STATUSES),
            )
        )
