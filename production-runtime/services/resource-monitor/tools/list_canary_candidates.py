"""List recent resource_candidates suitable for a future single-task Canary.

This tool is diagnostic only: it never creates a Resource, Queue Task, or transfer.
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.transfer.candidate_canary import list_canary_resource_candidates


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only resource_candidates Canary feasibility list")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    report = asyncio.run(list_canary_resource_candidates(hours=args.hours, limit=args.limit))
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
