import html
import logging
from typing import Any

import httpx

from app.core.config import get_settings
from app.models.resource import Resource

logger = logging.getLogger(__name__)


class TransferNotifier:
    """Delivers transfer success messages to original channels and failure alerts to the tracking bot admin."""

    def __init__(
        self,
        bot_token: str | None = None,
        admin_tg_id: int | None = None,
        default_channel_id: str | None = None,
    ) -> None:
        settings = get_settings()
        self.bot_token = bot_token if bot_token is not None else settings.telegram_bot_token
        self.admin_tg_id = admin_tg_id if admin_tg_id is not None else settings.admin_tg_id
        self.default_channel_id = default_channel_id if default_channel_id is not None else settings.channel_id

    async def _send_telegram(self, chat_id: str | int, text: str, reply_markup: dict | None = None) -> bool:
        if not self.bot_token or not chat_id:
            logger.debug('Telegram notification skipped: bot_token or chat_id not provided')
            return False
        url = f'https://api.telegram.org/bot{self.bot_token}/sendMessage'
        payload = {
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True,
        }
        if reply_markup:
            payload['reply_markup'] = reply_markup
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.post(url, json=payload)
                if res.status_code == 200 and res.json().get('ok'):
                    logger.info('Notification sent successfully to chat %s', chat_id)
                    return True
                logger.warning('Failed to send Telegram message to %s: %s', chat_id, res.text)
                return False
        except (httpx.HTTPError, OSError, ValueError) as exc:
            logger.warning('Exception sending Telegram message to %s: %s', chat_id, exc)
            return False

    async def notify_success(
        self,
        *,
        task_payload: dict[str, Any],
        transfer_result: dict[str, Any],
        resource: Resource | None = None,
    ) -> bool:
        """转存成功推送原频道 (source_channel_id 或配置的默认发布频道)."""
        target_channel = (
            task_payload.get('source_channel_id')
            or (resource.source_channel_id if resource else None)
            or self.default_channel_id
        )
        title = html.escape(str(task_payload.get('title') or (resource.title if resource else '影视资源')))
        season = task_payload.get('season') or (resource.season if resource else None)
        episode_keys = task_payload.get('episode_keys') or ([resource.episode_key] if resource and resource.episode_key else [])
        eps_str = ', '.join(episode_keys) if episode_keys else (f'S{season:02d}' if season else '全集')
        provider = html.escape(str(task_payload.get('provider') or (resource.cloud_name if resource else 'guangya')))
        remote_files = transfer_result.get('remote_files') or []
        files_preview = '\n'.join(f'  • {html.escape(f)}' for f in remote_files[:8])
        if len(remote_files) > 8:
            files_preview += f'\n  ...等共 {len(remote_files)} 个文件'

        msg = (
            f'🎉 <b>【影视转存成功】</b>\n\n'
            f'▫️ <b>片名:</b> {title}\n'
            f'▫️ <b>集数:</b> {eps_str}\n'
            f'▫️ <b>网盘:</b> {provider}\n'
            f'▫️ <b>校验状态:</b> 真实落盘校验通过 (Verified)\n'
        )
        if files_preview:
            msg += f'\n📁 <b>已转存文件:</b>\n{files_preview}\n'

        return await self._send_telegram(target_channel, msg)

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
        """转存失败推送追新bot (私聊 ADMIN_TG_ID) — 结构化中文原因 + 操作按钮."""
        if not self.admin_tg_id:
            logger.warning('Transfer failure alert skipped: ADMIN_TG_ID not configured')
            return False

        from app.transfer.failure_labels import category_zh, failure_markup, stage_zh

        title = html.escape(str(task_payload.get('title') or (resource.title if resource else '影视资源')))
        season = task_payload.get('season') or (resource.season if resource else None)
        episode_keys = task_payload.get('episode_keys') or ([resource.episode_key] if resource and resource.episode_key else [])
        eps_str = ', '.join(episode_keys) if episode_keys else (f'S{int(season):02d}' if season else '未知集数')
        share_url = html.escape(str(task_payload.get('share_url') or (resource.share_url if resource else '')))[:200]
        category_label = category_zh(category)
        stage_label = stage_zh(stage)
        status_line = f'HTTP {http_status}' if http_status else '—'
        tech = html.escape(str(error_message or '未知错误'))[:500]

        msg = (
            f'⚠️ <b>【影视转存失败】</b>\n\n'
            f'▫️ <b>片名:</b> {title}\n'
            f'▫️ <b>季/集:</b> {eps_str}\n'
            f'▫️ <b>任务ID:</b> <code>#{task_id}</code>\n'
            f'▫️ <b>失败阶段:</b> {stage_label}\n'
            f'▫️ <b>失败类型:</b> {category_label}\n'
            f'▫️ <b>HTTP状态:</b> <code>{status_line}</code>\n'
            f'▫️ <b>当前资源:</b> <code>{share_url}</code>\n'
            f'▫️ <b>尝试次数:</b> 第 {attempts} 次\n'
            f'▫️ <b>技术详情:</b> <code>{tech}</code>'
        )
        return await self._send_telegram(
            self.admin_tg_id,
            msg,
            reply_markup={'inline_keyboard': failure_markup(int(task_id) if task_id else 0)},
        )
