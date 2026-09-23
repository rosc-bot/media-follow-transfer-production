from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.transfer import TransferQueueTask

router=APIRouter(prefix='/transfer',tags=['transfer'])

@router.get('/queue')
async def list_queue(db: AsyncSession=Depends(get_db)):  # noqa: B008
    return [{'id':t.id,'resource_id':t.resource_id,'status':t.status,'attempt_count':t.attempt_count} for t in (await db.scalars(select(TransferQueueTask).order_by(TransferQueueTask.id.desc()).limit(100))).all()]
