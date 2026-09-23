import html
import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from app.core.config import get_settings
from app.models.resource import Resource

logger = logging.getLogger(__name__)

_NUMERIC_CHAT_ID = re.compile(r"^-?\d{1,20}$")
_USERNAME_CHAT_ID = re.compile(r"^@[A-Za-z][A-Za-z0-9_]{4,31}$")
_UNSET = object()


@dataclass(frozen=True)
class NotificationTarget:
    chat_id: str | int
    source: str


@dataclass(frozen=True)
class NotificationResult:
    status: str
    sent: bool
    target_chat_id: str | int | None = None
    target_source: str | None = None
    error: str | None = None
    telegram_message_id: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "sent": self.sent,
            "target_chat_id": self.target_chat_id,
            "target_source": self.target_source,
            "telegram_message_id": self.telegram_message_id,
            "error": self.error,
        }


@dataclass(frozen=True)
class SuccessCard:
    caption: str
    poster_url: str | None
    poster_status: str
    business_status: str
    selected_file_names: tuple[str, ...]
    selected_size_bytes: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "caption": self.caption,
            "poster_url": self.poster_url,
            "poster_status": self.poster_status,
            "business_status": self.business_status,
            "selected_file_names": list(self.selected_file_names),
            "selected_size_bytes": self.selected_size_bytes,
        }


_SENSITIVE_SHARE_QUERY_KEYS = frozenset({
    "access_token", "refresh_token", "auth", "auth_token", "authorization", "cookie", "api_key",
})


def sanitize_share_url(value: object) -> str:
    """Keep public share URLs readable while removing credential parameters."""

    raw = str(value or "").strip()
    if not raw or not raw.lower().startswith(("http://", "https://")):
        return ""
    parts = urlsplit(raw)
    query = [
        (key, item)
        for key, item in parse_qsl(parts.query, keep_blank_values=True)
        if key.casefold() not in _SENSITIVE_SHARE_QUERY_KEYS
    ]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


def _format_size(size_bytes: int) -> str:
    value = max(0, int(size_bytes or 0))
    if value >= 1024**3:
        return f"{value / 1024**3:.1f} GB"
    if value >= 1024**2:
        return f"{value / 1024**2:.1f} MB"
    if value >= 1024:
        return f"{value / 1024:.1f} KB"
    return f"{value} B"


def _episode_display(payload: dict[str, Any], names: tuple[str, ...]) -> tuple[str, int]:
    raw = payload.get("episode_keys") or []
    keys = [str(value).strip() for value in raw if str(value).strip()]
    if not keys:
        keys = [str(value).strip() for value in names if str(value).strip()]
    return ", ".join(keys) if keys else "本次入库", len(keys)


def _business_status(payload: dict[str, Any], transfer_result: dict[str, Any]) -> str:
    if str(payload.get("promotion_status") or transfer_result.get("promotion_status") or "").upper() == "PROMOTION_COMPLETED":
        return "已完结，已归档"
    status = str(payload.get("series_status") or payload.get("tmdb_series_status") or "").casefold()
    ended = status in {"ended", "canceled", "cancelled"}
    try:
        total = int(payload.get("total_episodes") or 0)
    except (TypeError, ValueError):
        total = 0
    collected = {
        str(value).strip()
        for value in payload.get("collected_episodes") or []
        if str(value).strip()
    }
    try:
        inventory_count = int(payload.get("inventory_count") or 0)
        cloud_count = int(payload.get("cloud_count") or 0)
        active_transfer_count = int(payload.get("active_transfer_count") or 0)
    except (TypeError, ValueError):
        inventory_count = cloud_count = active_transfer_count = 0
    content_complete = bool(payload.get("content_complete")) or (
        ended
        and total > 0
        and len(collected) >= total
        and inventory_count >= total
        and cloud_count >= total
        and active_transfer_count == 0
    )
    if ended:
        return "已收齐，等待归档" if content_complete else "已完结，补齐中"
    return "连载更新中"


def _specification(payload: dict[str, Any], names: tuple[str, ...]) -> str:
    raw = " ".join([str(payload.get("version_key") or ""), *names])
    patterns = (
        (r"(?i)(?:2160p|4k|uhd)", "4K"),
        (r"(?i)1080p", "1080P"),
        (r"(?i)720p", "720P"),
        (r"(?i)remux", "REMUX"),
        (r"(?i)web[- .]?dl", "WEB-DL"),
    )
    found = []
    for pattern, label in patterns:
        if re.search(pattern, raw) and label not in found:
            found.append(label)
    return " / ".join(found) or "其他"


def _contributor(payload: dict[str, Any]) -> str:
    for key in ("contributor_username", "source_username", "forwarded_username", "source_channel_username"):
        value = str(payload.get(key) or "").strip()
        if value.startswith("@") and re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{4,31}", value):
            return value
        if value and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{4,31}", value):
            return f"@{value}"
    title = str(payload.get("source_channel_title") or payload.get("source_channel_name") or "").strip()
    if title:
        return title
    source_type = str(payload.get("source_type") or "").casefold()
    if source_type == "framehdr":
        return "自动打捞 / FrameHDR"
    if source_type == "watchlist_scout":
        return "自动打捞 / Watchlist Scout"
    return "自动打捞"


def _poster_url(payload: dict[str, Any]) -> str | None:
    for key in ("poster_url", "watchlist_poster_url", "poster_path", "watchlist_poster_path"):
        value = str(payload.get(key) or "").strip()
        if not value:
            continue
        if value.startswith("/"):
            return f"https://image.tmdb.org/t/p/w500{value}"
        if value.lower().startswith(("http://", "https://")):
            return value
    return None


_POSTER_CACHE: dict[str, str | None] = {}


def _poster_value(value: object) -> str | None:
    if isinstance(value, dict):
        value = value.get("poster_url") or value.get("poster_path") or value.get("poster")
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.startswith("/"):
        return f"https://image.tmdb.org/t/p/w500{raw}"
    return raw if raw.lower().startswith(("http://", "https://")) else None


async def resolve_poster_url(
    payload: dict[str, Any],
    *,
    fetcher: Any | None = None,
) -> dict[str, str | None]:
    """Resolve a poster without changing transfer success semantics.

    The order is payload → watchlist fields → supplied TMDB cache/provider result
    → process cache → one TMDB read.  The process cache prevents repeated reads
    for the same identity during a worker lifetime; callers can also provide a
    persisted cache entry in ``tmdb_poster_cache``/``tmdb_details_cache``.
    """

    direct = _poster_url(payload)
    if direct:
        return {"url": direct, "status": "POSTER_AVAILABLE", "source": "payload_or_watchlist"}
    tmdb_id = str(payload.get("tmdb_id") or "").strip()
    for key in ("tmdb_poster_cache", "tmdb_details_cache", "tmdb_cache", "tmdb_provider_result"):
        cache = payload.get(key)
        if isinstance(cache, dict):
            value = cache.get(tmdb_id) if tmdb_id else None
            value = value if value is not None else cache
            poster = _poster_value(value)
            if poster:
                if tmdb_id:
                    _POSTER_CACHE[tmdb_id] = poster
                return {"url": poster, "status": "POSTER_AVAILABLE", "source": key}
    if tmdb_id and tmdb_id in _POSTER_CACHE:
        poster = _POSTER_CACHE[tmdb_id]
        return {
            "url": poster,
            "status": "POSTER_AVAILABLE" if poster else "POSTER_UNAVAILABLE",
            "source": "process_cache",
        }

    provider_result: object | None = None
    try:
        if fetcher is not None:
            provider_result = await fetcher(int(tmdb_id), payload) if tmdb_id else None
        else:
            settings = get_settings()
            if tmdb_id and settings.tmdb_api_key:
                media_type = str(payload.get("media_type") or "tv").casefold()
                endpoint = "movie" if media_type in {"movie", "电影"} else "tv"
                async with httpx.AsyncClient(timeout=8.0) as client:
                    response = await client.get(
                        f"{settings.tmdb_base_url.rstrip('/')}/{endpoint}/{tmdb_id}",
                        params={"api_key": settings.tmdb_api_key},
                    )
                    if response.status_code == 200:
                        provider_result = response.json()
    except (httpx.HTTPError, OSError, ValueError, TypeError):
        provider_result = None
    poster = _poster_value(provider_result)
    if tmdb_id:
        _POSTER_CACHE[tmdb_id] = poster
    return {
        "url": poster,
        "status": "POSTER_AVAILABLE" if poster else "POSTER_UNAVAILABLE",
        "source": "tmdb_provider" if poster else "none",
    }


def build_success_card(*, task_payload: dict[str, Any], transfer_result: dict[str, Any]) -> SuccessCard:
    """Build the user-facing legacy-style rich card from verified selected files."""

    selected = task_payload.get("selected_file_names") or task_payload.get("selected_verified_names")
    if not selected:
        selected = transfer_result.get("selected_file_names") or transfer_result.get("remote_files") or []
    names = tuple(dict.fromkeys(str(name).strip() for name in selected if str(name).strip()))
    sizes = transfer_result.get("selected_file_sizes") or task_payload.get("selected_file_sizes") or {}
    total_size = 0
    if isinstance(sizes, dict):
        for name in names:
            try:
                total_size += int(sizes.get(name) or 0)
            except (TypeError, ValueError):
                continue
    if not total_size:
        for record in transfer_result.get("remote_file_records") or []:
            if not isinstance(record, dict) or str(record.get("name") or record.get("fileName") or "").strip() not in names:
                continue
            try:
                total_size += int(record.get("size") or record.get("fileSize") or 0)
            except (TypeError, ValueError):
                continue

    title = html.escape(str(task_payload.get("title") or "影视资源"))
    region = html.escape(str(task_payload.get("region") or task_payload.get("country") or "未知"))
    media_type = "电影" if str(task_payload.get("media_type") or "tv").casefold() in {"movie", "电影"} else "剧集"
    episode_text, episode_count = _episode_display(task_payload, names)
    total = task_payload.get("total_episodes")
    count_text = f"{episode_text}（本次 {episode_count} 集"
    if total:
        count_text += f"，共 {int(total)} 集"
    count_text += "）"
    business_status = _business_status(task_payload, transfer_result)
    share_url = sanitize_share_url(task_payload.get("share_url") or task_payload.get("candidate_share_url"))
    share_line = f'<a href="{html.escape(share_url, quote=True)}">{html.escape(share_url)}</a>' if share_url else "未提供可用分享链接"
    destination = html.escape(str(task_payload.get("archive_directory") or task_payload.get("remote_rel_path") or task_payload.get("season_folder_name") or "影视转存总目录"))
    caption = (
        f"🎞 <b>片名：</b>{title}\n"
        f"📌 <b>地区：</b>{region}\n"
        f"📀 <b>类型：</b>{media_type}\n"
        f"📊 <b>状态：</b>{html.escape(business_status)}\n"
        f"📦 <b>收录集数：</b>{html.escape(count_text)}\n"
        f"🎞 <b>规格版本：</b>{html.escape(_specification(task_payload, names))}\n"
        f"💾 <b>资源体积：</b>{html.escape(_format_size(total_size))}\n"
        "☁️ <b>资源网盘：</b>#光鸭\n"
        f"🔗 <b>分享链接：</b>{share_line}\n"
        f"📁 <b>归档目录：</b>{destination}\n"
        f"📄 <b>已核验文件：</b>{html.escape(', '.join(names) or '—')}\n"
        f"✨ <b>智能去重：</b>✅ 转存成功（已确认 {len(names)} 个文件入库）\n"
        f"👤 <b>感谢贡献：</b>{html.escape(_contributor(task_payload))}\n"
        "✅ <b>入库验证：</b>已通过"
    )
    poster = _poster_url(task_payload)
    return SuccessCard(
        caption=caption,
        poster_url=poster,
        poster_status="POSTER_AVAILABLE" if poster else "POSTER_UNAVAILABLE",
        business_status=business_status,
        selected_file_names=names,
        selected_size_bytes=total_size,
    )


def build_promotion_card(*, task_payload: dict[str, Any], promotion_result: dict[str, Any]) -> str:
    """Preview-only archive notification; the worker never sends it in Phase 2G.4."""

    title = html.escape(str(task_payload.get("title") or "影视资源"))
    total = promotion_result.get("total_expected") or (task_payload.get("promotion_gate") or {}).get("total_expected") or "—"
    directory = html.escape(str(task_payload.get("archive_directory") or task_payload.get("series_folder_name") or "影视转存总目录"))
    return (
        "✅ <b>剧集已完结归档</b>\n\n"
        f"🎞 <b>片名：</b>{title}\n"
        f"📦 <b>总集数：</b>{html.escape(str(total))}\n"
        f"📁 <b>最终目录：</b>{directory}\n"
        "✅ <b>归档验证：</b>整剧目录 move + completed readback 已通过"
    )


class TransferNotifier:
    """Send transfer notifications through an explicit, validated target resolver.

    ``resource.source_channel_id`` is source provenance only. It is deliberately
    never considered a notification destination because Scout roles such as
    ``framehdr`` and ``watchlist_scout`` are not Telegram chat identifiers.
    """

    def __init__(
        self,
        bot_token: str | None = None,
        admin_tg_id: int | None | object = _UNSET,
        default_channel_id: str | None | object = _UNSET,
        success_chat: str | None | object = _UNSET,
    ) -> None:
        settings = get_settings()
        self.bot_token = bot_token if bot_token is not None else settings.telegram_bot_token
        self.admin_tg_id = settings.admin_tg_id if admin_tg_id is _UNSET else admin_tg_id
        self.default_channel_id = (
            settings.failure_notification_chat if default_channel_id is _UNSET else default_channel_id
        )
        self.success_chat = settings.transfer_success_chat if success_chat is _UNSET else success_chat

    @staticmethod
    def is_valid_chat_identifier(value: object) -> bool:
        if isinstance(value, bool) or value is None:
            return False
        text = str(value).strip()
        return bool(_NUMERIC_CHAT_ID.fullmatch(text) or _USERNAME_CHAT_ID.fullmatch(text))

    @classmethod
    def _normalize_chat_identifier(cls, value: object) -> str | int | None:
        if not cls.is_valid_chat_identifier(value):
            return None
        text = str(value).strip()
        return int(text) if _NUMERIC_CHAT_ID.fullmatch(text) else text

    def resolve_notification_target(
        self,
        task_payload: dict[str, Any],
        *,
        resource: Resource | None = None,
    ) -> NotificationTarget | None:
        """Resolve task/requester, subscriber, admin, then configured channel.

        ``resource.source_channel_id`` is provenance only. Invalid values are
        skipped so roles such as ``framehdr`` can never become chat targets.
        The configured success channel is deliberately not part of this failure
        chain; a failed transfer must go to the requester/subscriber/admin path.
        """
        candidates = (
            (task_payload.get("requester_chat_id"), "requester_chat_id"),
            (task_payload.get("subscriber_tg_id"), "subscriber_tg_id"),
            (task_payload.get("watchlist_subscriber_tg_id"), "watchlist_subscriber_tg_id"),
            # Kept only as a compatibility alias for old payloads; never a source id.
            (task_payload.get("notification_chat_id"), "legacy_notification_chat_id"),
            (task_payload.get("current_operator_tg_id"), "current_operator_tg_id"),
            (task_payload.get("operation_admin_tg_id"), "operation_admin_tg_id"),
            (self.admin_tg_id, "admin_tg_id"),
            (self.default_channel_id, "configured_notification_chat"),
        )
        for value, source in candidates:
            normalized = self._normalize_chat_identifier(value)
            if normalized is not None:
                return NotificationTarget(chat_id=normalized, source=source)
        return None

    @staticmethod
    def _telegram_error_code(status_code: int, body: object) -> str:
        description = ""
        if isinstance(body, dict):
            description = str(body.get("description") or body.get("error") or "").casefold()
        if status_code == 400 and "chat not found" in description:
            return "CHAT_NOT_FOUND"
        if "not enough rights" in description or "administrator rights" in description:
            return "INSUFFICIENT_RIGHTS"
        if status_code == 403 or "bot was blocked" in description or "forbidden" in description:
            return "BOT_FORBIDDEN"
        return "TELEGRAM_API_ERROR"

    async def _send_telegram_result(
        self,
        target: NotificationTarget | None,
        text: str,
        reply_markup: dict | None = None,
    ) -> NotificationResult:
        if target is None:
            logger.warning("Transfer notification target missing")
            return NotificationResult("NOTIFICATION_TARGET_MISSING", False, error="TARGET_INVALID")
        if not self.bot_token:
            logger.warning("Transfer notification failed: bot token missing")
            return NotificationResult(
                "NOTIFICATION_FAILED",
                False,
                target_chat_id=target.chat_id,
                target_source=target.source,
                error="BOT_TOKEN_MISSING",
            )
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": target.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.post(url, json=payload)
                try:
                    body = res.json()
                except ValueError:
                    body = {}
                if res.status_code == 200 and isinstance(body, dict) and body.get("ok"):
                    result = body.get("result")
                    if not isinstance(result, dict):
                        result = {}
                    raw_message_id = result.get("message_id")
                    message_id = int(raw_message_id) if isinstance(raw_message_id, int) else None
                    logger.info(
                        "transfer notification result status=SENT target_source=%s target_present=true",
                        target.source,
                    )
                    return NotificationResult(
                        "SENT",
                        True,
                        target_chat_id=target.chat_id,
                        target_source=target.source,
                        telegram_message_id=message_id,
                    )
                error_code = self._telegram_error_code(res.status_code, body)
                logger.warning(
                    "transfer notification result status=NOTIFICATION_FAILED target_source=%s http_status=%s error=%s",
                    target.source,
                    res.status_code,
                    error_code,
                )
                return NotificationResult(
                    "NOTIFICATION_FAILED",
                    False,
                    target_chat_id=target.chat_id,
                    target_source=target.source,
                    error=error_code,
                )
        except httpx.TimeoutException:
            logger.warning(
                "transfer notification result status=NOTIFICATION_FAILED target_source=%s error=NETWORK",
                target.source,
            )
            return NotificationResult(
                "NOTIFICATION_FAILED",
                False,
                target_chat_id=target.chat_id,
                target_source=target.source,
                error="NETWORK",
            )
        except (httpx.HTTPError, OSError):
            logger.warning(
                "transfer notification result status=NOTIFICATION_FAILED target_source=%s error=NETWORK",
                target.source,
            )
            return NotificationResult(
                "NOTIFICATION_FAILED",
                False,
                target_chat_id=target.chat_id,
                target_source=target.source,
                error="NETWORK",
            )
        except ValueError:
            logger.warning(
                "transfer notification result status=NOTIFICATION_FAILED target_source=%s error=TELEGRAM_API_ERROR",
                target.source,
            )
            return NotificationResult(
                "NOTIFICATION_FAILED",
                False,
                target_chat_id=target.chat_id,
                target_source=target.source,
                error="TELEGRAM_API_ERROR",
            )

    async def _send_telegram_photo_result(
        self,
        target: NotificationTarget | None,
        photo: str,
        caption: str,
    ) -> NotificationResult:
        if target is None:
            return NotificationResult("NOTIFICATION_TARGET_MISSING", False, error="TARGET_INVALID")
        if not self.bot_token:
            return NotificationResult(
                "NOTIFICATION_FAILED",
                False,
                target_chat_id=target.chat_id,
                target_source=target.source,
                error="BOT_TOKEN_MISSING",
            )
        url = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.post(
                    url,
                    json={
                        "chat_id": target.chat_id,
                        "photo": photo,
                        "caption": caption,
                        "parse_mode": "HTML",
                    },
                )
                body = res.json() if res.content else {}
                if res.status_code == 200 and isinstance(body, dict) and body.get("ok"):
                    result = body.get("result") or {}
                    message_id = result.get("message_id") if isinstance(result, dict) else None
                    return NotificationResult(
                        "SENT",
                        True,
                        target_chat_id=target.chat_id,
                        target_source=target.source,
                        telegram_message_id=int(message_id) if isinstance(message_id, int) else None,
                    )
                error = self._telegram_error_code(res.status_code, body)
                return NotificationResult(
                    "NOTIFICATION_FAILED",
                    False,
                    target_chat_id=target.chat_id,
                    target_source=target.source,
                    error=error,
                )
        except (httpx.HTTPError, OSError, ValueError):
            return NotificationResult(
                "NOTIFICATION_FAILED",
                False,
                target_chat_id=target.chat_id,
                target_source=target.source,
                error="NETWORK",
            )

    async def send_test_message(
        self,
        *,
        task_payload: dict[str, Any],
        text: str,
    ) -> NotificationResult:
        """Backward-compatible failure/operational test through the failure resolver."""
        target = self.resolve_notification_target(task_payload)
        return await self._send_telegram_result(target, html.escape(str(text)))

    async def send_success_test_message(self, *, text: str) -> NotificationResult:
        """Send one explicit success-channel test without touching any task."""
        return await self._send_telegram_result(self.resolve_success_target(), html.escape(str(text)))

    async def _send_telegram(self, chat_id: str | int, text: str, reply_markup: dict | None = None) -> bool:
        """Backward-compatible boolean wrapper for callers outside the worker."""
        target = NotificationTarget(chat_id=chat_id, source="explicit") if self.is_valid_chat_identifier(chat_id) else None
        return (await self._send_telegram_result(target, text, reply_markup)).sent

    def resolve_success_target(self, success_chat: object = _UNSET) -> NotificationTarget | None:
        """Return the fixed success-channel target, never a requester/admin fallback."""
        configured = self.success_chat if success_chat is _UNSET else success_chat
        normalized = self._normalize_chat_identifier(configured)
        if normalized is None:
            return None
        return NotificationTarget(chat_id=normalized, source="transfer_success_chat")

    async def notify_success_result(
        self,
        *,
        task_payload: dict[str, Any],
        transfer_result: dict[str, Any],
        resource: Resource | None = None,
    ) -> NotificationResult:
        # A success card is legal only after restore + authenticated readback.
        if not transfer_result.get("verified"):
            return NotificationResult(
                "NOTIFICATION_SKIPPED_UNVERIFIED",
                False,
                error="READBACK_NOT_VERIFIED",
            )
        # Success routing is fixed to the configured success channel.  A task
        # payload cannot redirect it to a requester/admin fallback.
        target = self.resolve_success_target()
        enriched_payload = dict(task_payload)
        if resource is not None:
            enriched_payload.setdefault("title", resource.title)
            enriched_payload.setdefault("tmdb_id", resource.tmdb_id)
            enriched_payload.setdefault("media_type", resource.media_type)
            enriched_payload.setdefault("season", resource.season)
            enriched_payload.setdefault("episode_keys", [resource.episode_key] if resource.episode_key else [])
            enriched_payload.setdefault("share_url", resource.share_url)
            enriched_payload.setdefault("source_type", resource.source_type)
        poster_resolution = await resolve_poster_url(enriched_payload)
        if poster_resolution.get("url"):
            enriched_payload["poster_url"] = poster_resolution["url"]
        card = build_success_card(task_payload=enriched_payload, transfer_result=transfer_result)
        if card.poster_url:
            photo_result = await self._send_telegram_photo_result(target, card.poster_url, card.caption)
            if photo_result.sent:
                return photo_result
            logger.warning("Poster notification failed; falling back to text: %s", photo_result.error)
        text_result = await self._send_telegram_result(target, card.caption)
        if text_result.sent and (card.poster_status == "POSTER_UNAVAILABLE" or not card.poster_url):
            return NotificationResult(
                text_result.status,
                text_result.sent,
                target_chat_id=text_result.target_chat_id,
                target_source=text_result.target_source,
                error="POSTER_UNAVAILABLE",
                telegram_message_id=text_result.telegram_message_id,
            )
        return text_result

    async def notify_success(
        self,
        *,
        task_payload: dict[str, Any],
        transfer_result: dict[str, Any],
        resource: Resource | None = None,
    ) -> bool:
        return (await self.notify_success_result(
            task_payload=task_payload,
            transfer_result=transfer_result,
            resource=resource,
        )).sent

    async def notify_failure_result(
        self,
        *,
        task_payload: dict[str, Any],
        error_message: str,
        resource: Resource | None = None,
        attempts: int = 1,
        task_id: int | None = None,
        category: str | None = None,
        stage: str | None = None,
        http_status: int | None = None,
    ) -> NotificationResult:
        from app.transfer.failure_labels import category_zh, failure_markup, stage_zh

        target = self.resolve_notification_target(task_payload, resource=resource)
        title = html.escape(str(task_payload.get("title") or (resource.title if resource else "影视资源")))
        season = task_payload.get("season") or (resource.season if resource else None)
        episode_keys = task_payload.get("episode_keys") or ([resource.episode_key] if resource and resource.episode_key else [])
        eps_str = ", ".join(str(key) for key in episode_keys) if episode_keys else (f"S{int(season):02d}" if season else "未知集数")
        raw_share_url = str(task_payload.get("share_url") or (resource.share_url if resource else ""))
        share_url = html.escape(raw_share_url)[:200]
        status_line = f"HTTP {http_status}" if http_status else "—"
        diagnostic = str(error_message or "原始错误未提供，无法进一步分类")
        share_line = f'<a href="{share_url}">打开当前资源</a>' if raw_share_url.startswith(("http://", "https://")) else "未提供可用资源链接"
        msg = (
            "⚠️ <b>【影视转存失败】</b>\n\n"
            f"▫️ <b>片名:</b> {title}\n"
            f"▫️ <b>季/集:</b> {eps_str}\n"
            f"▫️ <b>任务ID:</b> <code>#{task_id}</code>\n"
            f"▫️ <b>失败阶段:</b> {stage_zh(stage)}\n"
            f"▫️ <b>失败类型:</b> {category_zh(category)}\n"
            f"▫️ <b>HTTP状态:</b> <code>{status_line}</code>\n"
            f"▫️ <b>当前资源:</b> {share_line}\n"
            f"▫️ <b>尝试次数:</b> 第 {attempts} 次\n"
            f"▫️ <b>技术详情:</b> <code>{html.escape(diagnostic)[:500]}</code>"
        )
        return await self._send_telegram_result(
            target,
            msg,
            reply_markup={"inline_keyboard": failure_markup(int(task_id) if task_id else 0)},
        )

    async def notify_failure(
        self,
        *,
        task_payload: dict[str, Any],
        error_message: str,
        resource: Resource | None = None,
        attempts: int = 1,
        task_id: int | None = None,
        category: str | None = None,
        stage: str | None = None,
        http_status: int | None = None,
    ) -> bool:
        return (await self.notify_failure_result(
            task_payload=task_payload,
            error_message=error_message,
            resource=resource,
            attempts=attempts,
            task_id=task_id,
            category=category,
            stage=stage,
            http_status=http_status,
        )).sent
