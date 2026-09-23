from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.core.constants import SOURCE_TELEGRAM_CHANNEL


class TelegramSourceMessage(BaseModel):
    model_config = ConfigDict(extra='forbid')

    source_type: str = SOURCE_TELEGRAM_CHANNEL
    channel_id: str
    channel_username: str | None = None
    channel_title: str | None = None
    message_id: int
    text: str = ''
    caption: str = ''
    entities: list[dict[str, Any]] = Field(default_factory=list)
    urls: list[str] = Field(default_factory=list)
    button_urls: list[str] = Field(default_factory=list)
    published_at: datetime | None = None
    is_forward: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_telethon(cls, message: Any, chat: Any = None, *, source_type: str = SOURCE_TELEGRAM_CHANNEL) -> 'TelegramSourceMessage':
        chat_id = getattr(chat, 'id', None) if chat is not None else None
        chat_id = chat_id if chat_id is not None else getattr(message, 'chat_id', '')
        raw_text = getattr(message, 'text', None) or getattr(message, 'message', '') or ''
        is_media = bool(getattr(message, 'media', None))
        entities = []
        for ent in getattr(message, 'entities', None) or []:
            cls_name = ent.__class__.__name__
            entities.append({'type': 'text_link' if cls_name == 'MessageEntityTextUrl' else cls_name,
                             'offset': int(getattr(ent, 'offset', 0)), 'length': int(getattr(ent, 'length', 0)),
                             'url': getattr(ent, 'url', None), 'is_caption': is_media})
        buttons = []
        for row in getattr(message, 'buttons', None) or []:
            for button in row:
                url = getattr(button, 'url', None)
                if url:
                    buttons.append(str(url))
        from app.ingest.url_extractor import extract_urls
        urls = extract_urls(raw_text, entities, buttons)
        date = getattr(message, 'date', None) or datetime.now(UTC)
        is_fwd = bool(getattr(message, 'fwd_from', None) or getattr(message, 'forward', None)
                      or getattr(message, 'forward_date', None) or getattr(message, 'forward_origin', None))
        return cls(source_type=source_type, channel_id=str(chat_id or ''),
                   channel_username=getattr(chat, 'username', None),
                   channel_title=str(getattr(chat, 'title', None) or ''), message_id=int(getattr(message, 'id', 0)),
                   text='' if is_media else raw_text, caption=raw_text if is_media else '', entities=entities,
                   urls=urls, button_urls=buttons,
                   published_at=date, is_forward=is_fwd)

    @classmethod
    def from_aiogram(cls, message: Any, *, source_type: str = SOURCE_TELEGRAM_CHANNEL) -> 'TelegramSourceMessage':
        chat = getattr(message, 'chat', None)
        is_fwd = bool(getattr(message, 'forward_date', None) or getattr(message, 'forward_origin', None))
        return cls(source_type=source_type, channel_id=str(getattr(chat, 'id', '') or ''),
                   channel_username=getattr(chat, 'username', None), channel_title=str(getattr(chat, 'title', '') or ''),
                   message_id=int(getattr(message, 'message_id', 0)), text=getattr(message, 'text', '') or '',
                   caption=getattr(message, 'caption', '') or '', published_at=getattr(message, 'date', None),
                   is_forward=is_fwd)
