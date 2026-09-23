"""Read-only Telegram Bot API verification for fixed channels."""

from __future__ import annotations

from typing import Any

import httpx

from app.core.config import get_settings


class TelegramRouteVerifier:
    def __init__(self, *, bot_token: str | None = None, bot_username: str | None = None) -> None:
        settings = get_settings()
        self.bot_token = bot_token if bot_token is not None else settings.telegram_bot_token
        self.expected_username = (bot_username if bot_username is not None else settings.telegram_bot_username).lstrip("@")

    async def _get(self, method: str, params: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
        if not self.bot_token:
            return 0, {"ok": False, "description": "BOT_TOKEN_MISSING"}
        url = f"https://api.telegram.org/bot{self.bot_token}/{method}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url, params=params or {})
            try:
                body = response.json()
            except ValueError:
                body = {}
            return response.status_code, body if isinstance(body, dict) else {}
        except (httpx.HTTPError, OSError):
            return 0, {"ok": False, "description": "NETWORK"}

    @staticmethod
    def _api_error(status: int, body: dict[str, Any], *, default: str = "TELEGRAM_API_ERROR") -> str:
        desc = str(body.get("description") or "").casefold()
        if "chat not found" in desc:
            return "CHAT_NOT_FOUND"
        if "bot is not a member" in desc or "user not found" in desc:
            return "BOT_NOT_MEMBER"
        if status == 403 or "forbidden" in desc:
            return "BOT_FORBIDDEN"
        if "not enough rights" in desc or "administrator rights" in desc:
            return "INSUFFICIENT_RIGHTS"
        if desc == "bot_token_missing":
            return "BOT_TOKEN_MISSING"
        if desc == "network":
            return "TELEGRAM_API_ERROR"
        return default

    async def verify(self, chats: list[str] | None = None) -> dict[str, Any]:
        settings = get_settings()
        configured = chats or [settings.transfer_success_chat, settings.resource_publish_chat]
        http_status, me_body = await self._get("getMe")
        me = me_body.get("result") if isinstance(me_body.get("result"), dict) else {}
        bot_id = me.get("id")
        bot_username = str(me.get("username") or "")
        result: dict[str, Any] = {
            "sender_bot": f"@{bot_username}" if bot_username else None,
            "sender_bot_id_present": bot_id is not None,
            "identity_ok": bool(me_body.get("ok")) and bot_username.casefold() == self.expected_username.casefold(),
            "identity_error": None if me_body.get("ok") else self._api_error(http_status, me_body),
            "channels": {},
        }
        if me_body.get("ok") and bot_username.casefold() != self.expected_username.casefold():
            result["identity_error"] = "BOT_IDENTITY_MISMATCH"
        for target in configured:
            safe_target = str(target)
            chat_status, chat_body = await self._get("getChat", {"chat_id": safe_target})
            chat = chat_body.get("result") if isinstance(chat_body.get("result"), dict) else {}
            entry: dict[str, Any] = {
                "configured_target": safe_target,
                "chat_exists": bool(chat_body.get("ok")),
                "chat_id": chat.get("id"),
                "chat_type": chat.get("type"),
                "chat_username": chat.get("username"),
                "bot_membership": None,
                "bot_status": None,
                "is_admin": False,
                "can_post_messages": False,
                "can_edit_messages": False,
                "can_delete_messages": False,
                "can_manage_chat": False,
                "validation_code": None,
            }
            if not chat_body.get("ok"):
                entry["validation_code"] = self._api_error(chat_status, chat_body, default="CHAT_NOT_FOUND")
                result["channels"][safe_target] = entry
                continue
            member_status, member_body = await self._get("getChatMember", {"chat_id": chat.get("id") or safe_target, "user_id": bot_id})
            member = member_body.get("result") if isinstance(member_body.get("result"), dict) else {}
            status = str(member.get("status") or "")
            entry.update({
                "bot_membership": bool(member_body.get("ok")) and status in {"member", "administrator", "creator"},
                "bot_status": status or None,
                "is_admin": status in {"administrator", "creator"},
                "can_post_messages": bool(member.get("can_post_messages")),
                "can_edit_messages": bool(member.get("can_edit_messages")),
                "can_delete_messages": bool(member.get("can_delete_messages")),
                "can_manage_chat": bool(member.get("can_manage_chat")),
            })
            if not member_body.get("ok"):
                entry["validation_code"] = self._api_error(member_status, member_body, default="BOT_NOT_MEMBER")
            elif not entry["bot_membership"]:
                entry["validation_code"] = "BOT_NOT_MEMBER"
            elif not entry["is_admin"] or not entry["can_post_messages"]:
                entry["validation_code"] = "INSUFFICIENT_RIGHTS"
            else:
                entry["validation_code"] = "OK"
            result["channels"][safe_target] = entry
        result["all_channels_valid"] = bool(result["identity_ok"]) and all(
            row.get("validation_code") == "OK" for row in result["channels"].values()
        )
        return result
