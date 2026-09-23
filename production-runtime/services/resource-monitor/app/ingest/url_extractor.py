"""Telegram Resource Link Extractor (url_extractor.py).

Unified link extraction module for Telegram messages:
- Extracts plain text URLs (message.text) -> text_urls
- Extracts media caption URLs (message.caption / media text) -> caption_urls
- Extracts hidden text links (Telegram entities & markdown [text](url)) -> entity_urls
- Extracts inline keyboard button URLs (message.buttons) -> button_urls
- Merges all URLs with priority deduplication -> all_urls
- Identifies major cloud drive providers while retaining unknown domains.
"""

from __future__ import annotations

import re
import urllib.parse
from typing import Any

# Universal HTTP/HTTPS URL pattern
URL_REGEX = re.compile(
    r"https?://[^\s<>\"'`\u3000\u201c\u201d\u2018\u2019\uff08\uff09\u3010\u3011\u300a\u300b\uff0c\u3002\uff01\uff1f\uff1b\u3001^]+",
    re.IGNORECASE,
)

# Common cloud drive schemeless patterns (e.g. pan.quark.cn/s/..., pan.baidu.com/s/...)
CLOUD_DRIVE_SCHEMELESS_REGEX = re.compile(
    r"(?:(?:pan|drive)\.quark\.cn|(?:pan|yun)\.baidu\.com|(?:pan\.)?xunlei\.com|"
    r"(?:pan\.)?guangyapan\.com|(?:pan\.)?gypan\.com|guangya\.com|"
    r"cloud\.189\.cn|115\.com|anxia\.com|(?:www\.)?alipan\.com|(?:www\.)?aliyundrive\.com)"
    r"/(?:s|share|web)/[a-zA-Z0-9_\-.]+(?:[?#][^\s<>\"'`\u3000\u201c\u201d\u2018\u2019\uff08\uff09\u3010\u3011\u300a\u300b\uff0c\u3002\uff01\uff1f\uff1b\u3001^]*)?",
    re.IGNORECASE,
)

# Markdown link pattern: [label](url)
MD_LINK_REGEX = re.compile(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", re.IGNORECASE)

# Trailing punctuation characters to strip from URLs
TRAILING_PUNCT = ".,!?;:)>]\"'~`\u3000\u201c\u201d\u2018\u2019\uff08\uff09\u3010\u3011\u300a\u300b\uff0c\u3002\uff01\uff1f\uff1b\u3001^"
LEADING_PUNCT = "(\"'<[{<“‘（【《"

# Cloud drive provider domain mapping
CLOUD_DRIVE_PROVIDERS: dict[str, list[str]] = {
    "guangya": ["guangyapan.com", "gypan.com", "guangya.com"],
    "quark": ["pan.quark.cn", "drive.quark.cn", "quark.cn"],
    "baidu": ["pan.baidu.com", "yun.baidu.com"],
    "aliyun": ["alipan.com", "aliyundrive.com"],
    "115": ["115.com", "anxia.com"],
    "xunlei": ["pan.xunlei.com", "xunlei.com"],
    "tianyi": ["cloud.189.cn", "189.cn"],
}


def clean_url(raw: str) -> str:
    """Cleans punctuation surrounding a URL and ensures http/https scheme."""
    if not raw or not isinstance(raw, str):
        return ""
    u = raw.strip().lstrip(LEADING_PUNCT).rstrip(TRAILING_PUNCT).strip()
    u = u.removesuffix("?")
    if not u.lower().startswith(("http://", "https://")) and any(
        dom in u.lower() for domains in CLOUD_DRIVE_PROVIDERS.values() for dom in domains
    ):
        u = "https://" + u
    return u


def identify_cloud_drive(url: str) -> str | None:
    """Identifies the cloud drive provider for a given URL, or returns None if unknown.

    Note: Unknown domains are never restricted or dropped; this is purely an identifier helper.
    """
    if not url:
        return None
    try:
        if not url.lower().startswith(("http://", "https://")):
            url = "https://" + url
        parsed = urllib.parse.urlsplit(url)
        netloc = (parsed.netloc or "").lower()
    except Exception:  # noqa: BLE001
        return None

    for provider, domains in CLOUD_DRIVE_PROVIDERS.items():
        if any(dom in netloc for dom in domains):
            return provider
    return None


def _extract_from_text_block(text: str) -> tuple[list[str], list[str]]:
    """Extracts (plain_urls, markdown_hidden_urls) from a given text block."""
    if not text or not isinstance(text, str):
        return [], []

    md_hidden_urls: list[str] = []
    seen_md: set[str] = set()
    for _label, raw_url in MD_LINK_REGEX.findall(text):
        cleaned = clean_url(raw_url)
        if cleaned and cleaned.lower().startswith(("http://", "https://")) and cleaned not in seen_md:
            seen_md.add(cleaned)
            md_hidden_urls.append(cleaned)

    # Remove markdown link markup so plain URL regex does not duplicate hidden links
    text_plain = MD_LINK_REGEX.sub(r"\1", text)

    plain_urls: list[str] = []
    seen_plain: set[str] = set()

    for raw_u in URL_REGEX.findall(text_plain):
        cleaned = clean_url(raw_u)
        if cleaned and cleaned.lower().startswith(("http://", "https://")) and cleaned not in seen_plain:
            seen_plain.add(cleaned)
            plain_urls.append(cleaned)

    for raw_u in CLOUD_DRIVE_SCHEMELESS_REGEX.findall(text_plain):
        cleaned = clean_url(raw_u)
        if cleaned and cleaned.lower().startswith(("http://", "https://")) and cleaned not in seen_plain:
            seen_plain.add(cleaned)
            plain_urls.append(cleaned)

    return plain_urls, md_hidden_urls


def _extract_button_urls(message: Any) -> list[str]:
    """Safely extracts URLs from message buttons (Inline Keyboard).

    Gracefully ignores buttons without URLs or callback_data buttons without error.
    """
    raw_buttons: list[Any] = []

    # 1. Telethon style: message.buttons (list of lists, or list)
    btns = getattr(message, "buttons", None)
    if btns is not None:
        if isinstance(btns, list):
            for row in btns:
                if isinstance(row, list):
                    raw_buttons.extend(row)
                else:
                    raw_buttons.append(row)
        else:
            raw_buttons.append(btns)

    # 2. aiogram style: message.reply_markup.inline_keyboard
    reply_markup = getattr(message, "reply_markup", None)
    if reply_markup and hasattr(reply_markup, "inline_keyboard"):
        ik = getattr(reply_markup, "inline_keyboard", None)
        if isinstance(ik, list):
            for row in ik:
                if isinstance(row, list):
                    raw_buttons.extend(row)
                else:
                    raw_buttons.append(row)

    results: list[str] = []
    seen: set[str] = set()

    for btn in raw_buttons:
        if btn is None:
            continue
        url: str | None = None
        if isinstance(btn, dict):
            url = btn.get("url")
        else:
            url = getattr(btn, "url", None)

        if not url or not isinstance(url, str):
            continue

        cleaned = clean_url(url)
        if cleaned and cleaned.lower().startswith(("http://", "https://")) and cleaned not in seen:
            seen.add(cleaned)
            results.append(cleaned)

    return results


def extract_all_urls(message: Any) -> dict[str, list[str]]:
    """Extracts all resource links from a Telegram Message object in unified format.

    Extraction source priority:
    1. message.text (Plain text)
    2. message.caption (Media caption)
    3. message.entities (Hidden text links / entities)
    4. message.caption_entities (Media caption entities)
    5. message.buttons (Inline Keyboard URL buttons)

    Returns:
    {
        "text_urls": [],
        "caption_urls": [],
        "entity_urls": [],
        "button_urls": [],
        "all_urls": []
    }
    """
    empty_result: dict[str, list[str]] = {
        "text_urls": [],
        "caption_urls": [],
        "entity_urls": [],
        "button_urls": [],
        "all_urls": [],
    }

    if message is None:
        return empty_result

    # 1. Distinguish text vs caption based on message attributes
    caption_attr = getattr(message, "caption", None)
    text_attr = getattr(message, "text", None)
    msg_attr = getattr(message, "message", None)
    has_media = bool(
        getattr(message, "media", None)
        or getattr(message, "photo", None)
        or getattr(message, "video", None)
        or getattr(message, "document", None)
        or getattr(message, "audio", None)
        or getattr(message, "voice", None)
    )

    text_str = ""
    caption_str = ""

    if caption_attr:
        caption_str = str(caption_attr)
        if text_attr and str(text_attr) != caption_str:
            text_str = str(text_attr)
    elif has_media:
        caption_str = str(text_attr or msg_attr or "")
    else:
        text_str = str(text_attr or msg_attr or "")

    # 2. Extract from text and caption
    text_urls, text_md_entities = _extract_from_text_block(text_str)
    caption_urls, caption_md_entities = _extract_from_text_block(caption_str)

    # 3. Extract from Telegram entity objects (message.entities & message.caption_entities)
    entity_urls: list[str] = []
    seen_entity: set[str] = set()

    for md_u in text_md_entities + caption_md_entities:
        if md_u not in seen_entity:
            seen_entity.add(md_u)
            entity_urls.append(md_u)

    raw_entities: list[Any] = []
    msg_entities = getattr(message, "entities", None)
    if isinstance(msg_entities, list):
        raw_entities.extend(msg_entities)
    caption_entities = getattr(message, "caption_entities", None)
    if isinstance(caption_entities, list):
        raw_entities.extend(caption_entities)

    for ent in raw_entities:
        if ent is None:
            continue
        ent_url = None
        if isinstance(ent, dict):
            ent_url = ent.get("url")
            ent_type = ent.get("type", "")
            if not ent_url and ent_type in ("text_link", "MessageEntityTextUrl"):
                ent_url = ent.get("url")
        else:
            ent_url = getattr(ent, "url", None)

        if ent_url and isinstance(ent_url, str):
            cleaned = clean_url(ent_url)
            if cleaned and cleaned.lower().startswith(("http://", "https://")) and cleaned not in seen_entity:
                seen_entity.add(cleaned)
                entity_urls.append(cleaned)

    # 4. Extract from buttons
    button_urls = _extract_button_urls(message)

    # 5. Merge all_urls in priority order with deduplication
    all_urls: list[str] = []
    seen_all: set[str] = set()

    for u in text_urls + caption_urls + entity_urls + button_urls:
        if u not in seen_all:
            seen_all.add(u)
            all_urls.append(u)

    return {
        "text_urls": text_urls,
        "caption_urls": caption_urls,
        "entity_urls": entity_urls,
        "button_urls": button_urls,
        "all_urls": all_urls,
    }


def extract_urls(text: str, entities: list[dict] | None = None, button_urls: list[str] | None = None) -> list[str]:
    """Compatibility helper matching the simpler extract_urls interface."""
    dummy = type("DummyMsg", (), {
        "text": text,
        "caption": None,
        "message": text,
        "media": None,
        "entities": entities or [],
        "caption_entities": [],
        "buttons": [[type("DummyBtn", (), {"url": u}) for u in (button_urls or [])]],
        "reply_markup": None,
    })()
    res = extract_all_urls(dummy)
    return res["all_urls"]
