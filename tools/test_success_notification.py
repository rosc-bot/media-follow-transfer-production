"""Send exactly one explicit success-channel test through TransferNotifier."""

import argparse
import asyncio
import json

from app.transfer.notifier import TransferNotifier

TEST_TEXT = "🔧 影视转存通知通道测试\n\n【测试消息】\n@zhuixin001_bot → @guangyazhauncun\n生产通知链路验证成功。"


async def send() -> dict:
    notifier = TransferNotifier()
    result = await notifier.send_success_test_message(text=TEST_TEXT)
    target = notifier.resolve_success_target()
    return {
        "result": "SUCCESS_NOTIFICATION_TEST_SENT" if result.sent else "SUCCESS_NOTIFICATION_TEST_FAILED",
        "target_source": result.target_source,
        "target_valid": bool(target),
        "send_status": result.status,
        "telegram_message_id": result.telegram_message_id,
        "error": result.error,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirm-test-message", action="store_true", required=True)
    args = parser.parse_args()
    del args
    output = asyncio.run(send())
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if output["result"] == "SUCCESS_NOTIFICATION_TEST_SENT" else 1


if __name__ == "__main__":
    raise SystemExit(main())
