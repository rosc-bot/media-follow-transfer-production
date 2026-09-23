from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.follow.watchlist_service import WatchlistService

router=APIRouter(prefix='/follow',tags=['follow'])

@router.post('/watchlist')
async def add_watchlist(payload: dict, db: AsyncSession=Depends(get_db)):  # noqa: B008
    row=await WatchlistService.add(db, **payload)
    await db.commit()
    return {'id':row.id,'tmdb_id':row.tmdb_id,'title':row.title,'season':row.season}
