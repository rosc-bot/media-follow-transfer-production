from typing import Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import CHANNEL_ROLE_RESOURCE, TRANSFER_MODE_OFF
from app.core.database import get_db
from app.models.channel import ChannelSetting

router = APIRouter(prefix='/channels', tags=['channels'])


class ChannelSettingInput(BaseModel):
    channel_id: str = Field(min_length=1, max_length=128)
    channel_name: str | None = Field(default=None, max_length=512)
    enabled: bool = True
    role: Literal['RESOURCE', 'MANUAL_INGEST'] = CHANNEL_ROLE_RESOURCE
    transfer_mode: Literal['OFF', 'MANUAL', 'AUTO'] = TRANSFER_MODE_OFF
    accept_forward: bool = False
    default_provider: str | None = Field(default=None, max_length=64)
    default_category: str | None = Field(default=None, max_length=128)


def serialize_channel(row: ChannelSetting) -> dict:
    return {
        'channel_id': row.channel_id,
        'channel_name': row.channel_name,
        'role': row.role,
        'enabled': row.enabled,
        'accept_forward': row.accept_forward,
        'transfer_mode': row.transfer_mode,
        'default_provider': row.default_provider,
        'default_category': row.default_category,
    }


@router.get('')
async def list_channels(db: AsyncSession = Depends(get_db)):  # noqa: B008
    rows = (await db.scalars(select(ChannelSetting).order_by(ChannelSetting.channel_id))).all()
    return [serialize_channel(row) for row in rows]


@router.post('')
async def upsert_channel(payload: ChannelSettingInput, db: AsyncSession = Depends(get_db)):  # noqa: B008
    """Create or update only this new project's resource/manual-ingest configuration."""
    row = await db.scalar(select(ChannelSetting).where(ChannelSetting.channel_id == payload.channel_id))
    values = payload.model_dump()
    if row is None:
        row = ChannelSetting(**values)
        db.add(row)
    else:
        for field, value in values.items():
            setattr(row, field, value)
    await db.commit()
    await db.refresh(row)
    return serialize_channel(row)
