"""One-shot, safe backfill of pre-migration ongoing series roots.

This operation only records uniquely identified `{tmdbid-N}` directories in the
new database. It never queues a promotion, restores a share, moves a file, or
sends Telegram output.
"""

import asyncio

from app.core.database import AsyncSessionLocal
from app.follow.follow_worker import _list_legacy_directories
from app.follow.legacy_root_discovery_service import LegacyRootDiscoveryService


async def run() -> int:
    async with AsyncSessionLocal() as db, db.begin():
        return await LegacyRootDiscoveryService(_list_legacy_directories).backfill(db)


if __name__ == '__main__':
    print(asyncio.run(run()))
