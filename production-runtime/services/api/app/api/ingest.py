from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.ingest.channel_ingest_service import ChannelIngestService
from app.models.channel import ChannelSetting
from app.schemas.telegram_source import TelegramSourceMessage

router = APIRouter(prefix='/ingest', tags=['ingest'])


@router.post('/source-message')
async def ingest_source(source: TelegramSourceMessage, db: AsyncSession = Depends(get_db)):  # noqa: B008
    setting = await db.scalar(select(ChannelSetting).where(ChannelSetting.channel_id == source.channel_id))
    result = await ChannelIngestService.process_source_message(db, source, channel_setting=setting)
    await db.commit()
    return result
