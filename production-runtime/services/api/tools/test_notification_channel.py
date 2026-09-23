"""Send exactly one explicit production notification-channel test.

This tool intentionally calls the same ``TransferNotifier`` resolver and
Telegram Bot API implementation used by the transfer worker.  It never reads
or mutates a transfer task and never sends a #1301 success notification.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from app.transfer.notifier import TransferNotifier

TEST_TEXT = "🔧 影视转存通知通道测试\n\n【测试消息】\n@zhuixin001_bot → @guangyazhauncun\n生产通知链路验证成功。"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send one production Telegram notification test")
    parser.add_argument(
        "--confirm-test-message",
        action="store_true",
        help="required safety acknowledgement; this sends one Telegram message",
    )
    return parser


async def run() -> dict:
    notifier = TransferNotifier()
    target = notifier.resolve_success_target()
    result = await notifier.send_success_test_message(text=TEST_TEXT)
    return {
        "result": "SUCCESS_NOTIFICATION_TEST_SENT" if result.sent else "SUCCESS_NOTIFICATION_TEST_FAILED",
        "resolved_target_source": result.target_source,
        "resolved_target_valid": bool(target and notifier.is_valid_chat_identifier(target.chat_id)),
        "send_status": result.status,
        "telegram_message_id": result.telegram_message_id,
        "error": result.error,
        "source_channel_used_as_target": False,
    }


def main() -> int:
    args = _parser().parse_args()
    if not args.confirm_test_message:
        print(json.dumps({"result": "CONFIRMATION_REQUIRED"}, ensure_ascii=False))
        return 2
    try:
        output = asyncio.run(run())
    except Exception as exc:  # noqa: BLE001 - stable safe operational output
        output = {
            "result": "SUCCESS_NOTIFICATION_TEST_FAILED",
            "resolved_target_source": None,
            "resolved_target_valid": False,
            "send_status": "NOTIFICATION_FAILED",
            "telegram_message_id": None,
            "error": type(exc).__name__,
            "source_channel_used_as_target": False,
        }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if output["result"] == "SUCCESS_NOTIFICATION_TEST_SENT" else 1


if __name__ == "__main__":
    sys.exit(main())
