from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.resource import Resource

router=APIRouter(prefix='/resources',tags=['resources'])

@router.get('')
async def list_resources(db: AsyncSession=Depends(get_db)):  # noqa: B008
    return [{'id':r.id,'tmdb_id':r.tmdb_id,'title':r.title,'status':r.status} for r in (await db.scalars(select(Resource).order_by(Resource.id.desc()).limit(100))).all()]
