import asyncio
import json
import sqlite3
from pathlib import Path

from sqlalchemy import func, select

from app.core.database import AsyncSessionLocal
from app.models.ingest import ChannelIngestJob
from app.models.resource import Resource
from app.models.transfer import TransferJob
from app.models.watchlist import SeriesWatchlist


def old_watchlist_count(path: str) -> int:
    uri=f'file:{Path(path).resolve()}?mode=ro'
    with sqlite3.connect(uri, uri=True) as db: return db.execute('select count(*) from watchlist').fetchone()[0]


async def verify(path: str) -> dict:
    async with AsyncSessionLocal() as db:
        counts={
            'new_watchlist': await db.scalar(select(func.count()).select_from(SeriesWatchlist)),
            'resources': await db.scalar(select(func.count()).select_from(Resource)),
            'ingest_jobs': await db.scalar(select(func.count()).select_from(ChannelIngestJob)),
            'transfer_jobs': await db.scalar(select(func.count()).select_from(TransferJob)),
        }
    counts['old_watchlist']=old_watchlist_count(path)
    counts['watchlist_not_less_than_source']=counts['new_watchlist'] >= counts['old_watchlist']
    return counts


if __name__ == '__main__':
    import argparse
    parser=argparse.ArgumentParser(); parser.add_argument('path'); args=parser.parse_args()
    print(json.dumps(asyncio.run(verify(args.path)),ensure_ascii=False))
