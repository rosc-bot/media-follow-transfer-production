"""Telegram channel verifier CLI; read-only Bot API calls only."""

import argparse
import asyncio
import json

from app.telegram.route_verifier import TelegramRouteVerifier


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chat", action="append", dest="chats", help="override one target chat")
    args = parser.parse_args()
    result = asyncio.run(TelegramRouteVerifier().verify(args.chats))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("all_channels_valid") else 1


if __name__ == "__main__":
    raise SystemExit(main())
