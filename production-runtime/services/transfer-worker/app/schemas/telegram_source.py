from datetime import UTC, datetime
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.core.constants import SOURCE_TELEGRAM_CHANNEL


_TMDB_ID = re.compile(r'(?i)\btmdb(?:\s*id)?\b[^0-9]{0,32}(\d{2,10})')
_SEASON = re.compile(r'第\s*(\d{1,3})\s*季')
_SHORT_EPISODE = re.compile(r'(?i)(?<![a-z0-9])e(\d{1,4})(?!\d)')
_EPISODE_RANGE = re.compile(r'第\s*(\d{1,4})\s*[-~～至到]\s*(\d{1,4})\s*集')
_COMPLETE_PACK = re.compile(r'(\d{1,3})\s*集全')
_YEAR = re.compile(r'(?:上映\s*/\s*首播|上映|年份)[^0-9]{0,24}((?:19|20)\d{2})')
_TITLE_SPLIT = re.compile(
    r'\s+(?=(?:web[- ]?dl|web|blu-?ray|remux|4k|2160p|1080p|s\d{1,3}[ ._-]*e\d{1,4}|e\d{1,4}\b|第\s*\d+\s*季|\d+\s*集全)\b)|[·|]',
    re.IGNORECASE,
)


def extract_embedded_media_metadata(raw_text: str) -> dict[str, Any]:
    """Extract only explicit, channel-provided identity evidence from a resource post."""
    text = raw_text or ''
    metadata: dict[str, Any] = {}

    tmdb_match = _TMDB_ID.search(text)
    if tmdb_match:
        metadata['tmdb_id'] = int(tmdb_match.group(1))

    year_match = _YEAR.search(text)
    if year_match:
        metadata['year'] = int(year_match.group(1))

    from app.ingest.media_identity import clean_title
    clean_t = clean_title(text)
    if clean_t:
        metadata['title'] = clean_t
    else:
        first_line = text.splitlines()[0] if text.splitlines() else ''
        title = re.sub(r'^[🎬📺]\s*', '', first_line).replace('**', '').replace('`', '').strip()
        title = _TITLE_SPLIT.split(title, maxsplit=1)[0].strip(' -_:：')
        if 1 <= len(title) <= 256:
            metadata['title'] = title

    from app.ingest.episode_parser import parse_episode_keys

    episode_keys = parse_episode_keys(text)
    season_match = _SEASON.search(text)
    season = int(season_match.group(1)) if season_match else None
    short_episode_match = _SHORT_EPISODE.search(text)
    if short_episode_match:
        season = season or 1
        episode_keys.append(f'S{season:02d}E{int(short_episode_match.group(1)):02d}')
    episode_range_match = _EPISODE_RANGE.search(text)
    if episode_range_match:
        season = season or 1
        first, last = (int(value) for value in episode_range_match.groups())
        if first <= last <= first + 199:
            episode_keys.extend(f'S{season:02d}E{episode:02d}' for episode in range(first, last + 1))
    complete_pack_match = _COMPLETE_PACK.search(text)
    if complete_pack_match:
        season = season or 1
        count = int(complete_pack_match.group(1))
        episode_keys.extend(f'S{season:02d}E{episode:02d}' for episode in range(1, count + 1))
    if season is not None:
        metadata['season'] = season
    if episode_keys:
        metadata['episode_keys'] = sorted(set(episode_keys))

    is_tv = bool(metadata.get('episode_keys')) or bool(re.search(r'剧集|电视剧|国产剧|欧美剧|日韩剧|动画|第\s*\d+\s*季|\d+\s*集', text))
    if not is_tv and re.search(r'电影|影片|\bmovie\b', text, re.IGNORECASE):
        metadata['is_movie'] = True
    return metadata


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
        chat_id = getattr(message, 'chat_id', None)
        if chat_id is None:
            chat_id = getattr(chat, 'id', '') if chat is not None else ''
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
                   urls=urls, button_urls=buttons, metadata=extract_embedded_media_metadata(raw_text),
                   published_at=date, is_forward=is_fwd)

    @classmethod
    def from_aiogram(cls, message: Any, *, source_type: str = SOURCE_TELEGRAM_CHANNEL) -> 'TelegramSourceMessage':
        chat = getattr(message, 'chat', None)
        is_fwd = bool(getattr(message, 'forward_date', None) or getattr(message, 'forward_origin', None))
        raw_text = (getattr(message, 'text', '') or getattr(message, 'caption', '') or '')
        return cls(source_type=source_type, channel_id=str(getattr(chat, 'id', '') or ''),
                   channel_username=getattr(chat, 'username', None), channel_title=str(getattr(chat, 'title', '') or ''),
                   message_id=int(getattr(message, 'message_id', 0)), text=getattr(message, 'text', '') or '',
                   caption=getattr(message, 'caption', '') or '', metadata=extract_embedded_media_metadata(raw_text),
                   published_at=getattr(message, 'date', None), is_forward=is_fwd)
