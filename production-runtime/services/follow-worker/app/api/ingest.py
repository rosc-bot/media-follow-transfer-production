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
    raw_id = str(source.channel_id).strip()
    cand_ids = [raw_id]
    if raw_id.startswith("-100"):
        cand_ids.append(raw_id[4:])
    else:
        cand_ids.append(f"-100{raw_id.lstrip('-')}")
    setting = await db.scalar(select(ChannelSetting).where(ChannelSetting.channel_id.in_(cand_ids)))
    result = await ChannelIngestService.process_source_message(db, source, channel_setting=setting)
    await db.commit()
    return result
