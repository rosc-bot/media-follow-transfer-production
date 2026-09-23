from app.core.constants import (
    CHANNEL_ROLE_MANUAL_INGEST,
    SOURCE_MANUAL_FORWARD,
    SOURCE_TELEGRAM_CHANNEL,
)
from app.schemas.telegram_source import TelegramSourceMessage


def serialize_telegram_message(message, chat=None, setting=None) -> dict:
    role = getattr(setting, 'role', None)
    source_type = SOURCE_MANUAL_FORWARD if role == CHANNEL_ROLE_MANUAL_INGEST else SOURCE_TELEGRAM_CHANNEL
    source = TelegramSourceMessage.from_telethon(message, chat, source_type=source_type)
    return source.model_dump(mode='json')
