"""Event Router for Demuxing Telegram Messages into Summary & Resource Queues."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.core.constants import CHANNEL_ROLE_MANUAL_INGEST, CHANNEL_ROLE_RESOURCE

logger = logging.getLogger(__name__)


def normalize_id(raw_id: int | str | None) -> tuple[str, str]:
    """Returns (full_id, short_id) for reliable channel/group ID matching."""
    if raw_id is None:
        return "", ""
    s = str(raw_id).strip()
    clean = s.lstrip("-")
    if clean.startswith("100") and len(clean) > 3:
        short = clean[3:]
    else:
        short = clean
    full = f"-100{short}" if short else ""
    return full, short


class EventRouter:
    def __init__(
        self,
        summary_queue: asyncio.Queue,
        resource_queue: asyncio.Queue,
        settings_by_channel: dict[str, Any] | None = None,
    ):
        self.summary_queue = summary_queue
        self.resource_queue = resource_queue
        self.settings_by_channel = settings_by_channel or {}

    def update_settings(self, settings_by_channel: dict[str, Any]) -> None:
        self.settings_by_channel = settings_by_channel

    def find_setting(self, chat_id: int | str | None, username: str | None = None) -> Any | None:
        if not self.settings_by_channel:
            return None
        full_id, short_id = normalize_id(chat_id)
        uname = (username or "").lower().lstrip("@")

        for key, setting in self.settings_by_channel.items():
            k_full, k_short = normalize_id(key)
            k_uname = str(key).lower().lstrip("@")

            if full_id and k_full == full_id:
                return setting
            if short_id and k_short == short_id:
                return setting
            if uname and k_uname == uname:
                return setting
        return None

    async def route_event(self, event: Any, client: Any = None) -> None:
        """Route incoming Telethon event into isolated Summary or Resource queues."""
        try:
            msg = getattr(event, "message", None)
            if msg is None:
                return

            chat = await event.get_chat() if callable(getattr(event, "get_chat", None)) else getattr(event, "chat", None)
            chat_id = getattr(event, "chat_id", None)
            if chat_id is None and chat is not None:
                chat_id = getattr(chat, "id", None)

            if chat_id is None:
                return

            username = getattr(chat, "username", None) or getattr(getattr(event, "chat", None), "username", None)
            chat_title = getattr(chat, "title", None) or getattr(chat, "first_name", None) or str(chat_id)
            is_broadcast = bool(getattr(chat, "broadcast", False))
            is_private = bool(getattr(chat, "first_name", None) and not is_broadcast and not getattr(chat, "title", None))

            # Find if this chat matches any resource or manual ingest setting
            setting = self.find_setting(chat_id, username=username)

            # 1. Resource Routing
            # Matches if it's a broadcast channel with setting, or a group configured as a resource group
            if setting and getattr(setting, "enabled", True):
                role = getattr(setting, "role", CHANNEL_ROLE_RESOURCE)
                if role in (CHANNEL_ROLE_RESOURCE, CHANNEL_ROLE_MANUAL_INGEST):
                    try:
                        self.resource_queue.put_nowait({
                            "message": msg,
                            "chat": chat,
                            "setting": setting,
                            "message_id": getattr(msg, "id", None),
                        })
                    except Exception as e:  # noqa: BLE001
                        logger.error("Could not enqueue to resource_queue: %s", e)

            # 2. Summary Routing
            # All groups and supergroups (including resource groups) go to summary_queue
            if not is_broadcast and not is_private:
                try:
                    self.summary_queue.put_nowait({
                        "chat_id": int(chat_id),
                        "chat_title": str(chat_title),
                        "message": msg,
                        "kind": "group",
                        "message_id": getattr(msg, "id", None),
                    })
                except Exception as e:  # noqa: BLE001
                    logger.error("Could not enqueue to summary_queue: %s", e)

        except Exception:
            # Routing error MUST NEVER crash the Telegram listener
            logger.exception("Error routing Telegram event")
