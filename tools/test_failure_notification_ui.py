"""Send exactly one failure-card UI test to the configured OWNER fallback."""

import argparse
import asyncio
import json

from app.transfer.notifier import TransferNotifier


async def send() -> dict:
    notifier = TransferNotifier()
    payload = {
        "title": "通知 UI 通道测试",
        "season": 1,
        "episode_keys": ["S01E01"],
        "source_channel_id": "framehdr",
        "share_url": "https://pan.guangyapan.com/s/ui-test",
    }
    result = await notifier.notify_failure_result(
        task_payload=payload,
        error_message="【测试消息，不代表真实转存失败】reply_markup callback UI 验证",
        attempts=1,
        task_id=0,
        category="NETWORK_ERROR",
        stage="readback",
    )
    return {
        "result": "FAILURE_UI_TEST_SENT" if result.sent else "FAILURE_UI_TEST_FAILED",
        "target_source": result.target_source,
        "send_status": result.status,
        "telegram_message_id": result.telegram_message_id,
        "error": result.error,
        "source_channel_used_as_target": result.target_source == "source_channel_id",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirm-test-message", action="store_true", required=True)
    args = parser.parse_args()
    del args
    output = asyncio.run(send())
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if output["result"] == "FAILURE_UI_TEST_SENT" else 1


if __name__ == "__main__":
    raise SystemExit(main())
