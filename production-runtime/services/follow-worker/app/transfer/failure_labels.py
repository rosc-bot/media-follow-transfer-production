"""Transfer error taxonomy → human/Chinese labels + notification buttons.

Phase 2B: failure notifications must never say just "未知错误".  This module
owns the category → Chinese label/description map and the inline-button
callback ids shared by notifier and bot.
"""

from __future__ import annotations

from app.transfer.errors import TransferErrorCategory

CATEGORY_ZH: dict[str, str] = {
    TransferErrorCategory.AUTH_EXPIRED: '光鸭凭证已过期/刷新失败',
    TransferErrorCategory.AUTH_INVALID: '光鸭认证无效',
    TransferErrorCategory.INVALID_SHARE: '分享链接无效',
    TransferErrorCategory.SHARE_NOT_FOUND: '分享不存在或已失效',
    TransferErrorCategory.EMPTY_SHARE: '分享内容为空',
    TransferErrorCategory.NO_VIDEO_FILES: '分享中没有视频文件',
    TransferErrorCategory.EPISODE_MISMATCH: '资源剧集与目标不匹配',
    TransferErrorCategory.RATE_LIMITED: '接口限流（429）',
    TransferErrorCategory.NETWORK_TIMEOUT: '网络超时',
    TransferErrorCategory.NETWORK_ERROR: '网络错误',
    TransferErrorCategory.REMOTE_5XX: '远端服务异常（5xx）',
    TransferErrorCategory.READBACK_UNVERIFIED: '落盘校验未通过',
    TransferErrorCategory.DESTINATION_NOT_FOUND: '目标目录不存在',
    TransferErrorCategory.INSUFFICIENT_SPACE: '网盘空间不足',
    TransferErrorCategory.UNKNOWN: '未分类故障',
}

#: Failure stage labels (informative, not tied to the error taxonomy).
STAGE_ZH: dict[str, str] = {
    'claim': '任务认领',
    'auth': '认证',
    'list_directory': '目标目录读取',
    'ensure_directory': '目标目录创建',
    'restore': '转存执行',
    'readback': '落盘校验',
    'finalize': '结果入库',
    'unknown': '未知阶段',
}


def category_zh(category: object) -> str:
    return CATEGORY_ZH.get(str(category or ''), '未分类故障')


def stage_zh(stage: object) -> str:
    return STAGE_ZH.get(str(stage or ''), str(stage or '未标注阶段'))


#: Inline keyboard callback data (must stay ≤ 64 bytes).
BTN_RETRY = 'tx_fail_retry'
BTN_SWITCH = 'tx_fail_switch'
BTN_RESCOUT = 'tx_fail_rescout'
BTN_CANCEL = 'tx_fail_cancel'
BTN_IGNORE = 'tx_fail_ignore'


def failure_markup(task_id: int) -> list[list[dict]]:
    """Inline keyboard rows for a failure notification on *task_id*."""
    return [
        [
            {'text': '🔄 重试当前资源', 'callback_data': f'{BTN_RETRY}:{task_id}'},
            {'text': '🔗 更换资源', 'callback_data': f'{BTN_SWITCH}:{task_id}'},
        ],
        [
            {'text': '🔎 重新打捞', 'callback_data': f'{BTN_RESCOUT}:{task_id}'},
            {'text': '🛑 取消任务', 'callback_data': f'{BTN_CANCEL}:{task_id}'},
        ],
        [
            {'text': '🚫 忽略本集', 'callback_data': f'{BTN_IGNORE}:{task_id}'},
        ],
    ]
