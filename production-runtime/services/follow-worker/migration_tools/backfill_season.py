"""Narrow Phase 2D season backfill.

Default is a dry-run.  ``--apply`` requires an explicit confirmation and fills
only null ``resources.season`` / missing ``task.payload.season`` from freshly
recomputed SAFE_INFER audit rows.  It never changes task status, attempts,
identity, URLs, timestamps, or non-empty season values.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from app.core.database import AsyncSessionLocal
from app.season_audit import audit_missing_seasons
from app.season_backfill import apply_safe_backfill


def _projection(report: dict[str, Any]) -> list[dict[str, Any]]:
    keys = (
        "task_id", "resource_id", "tmdb_id", "title", "episode_key", "payload_season",
        "resource_season", "inferred_season", "confidence", "task_status", "source_type",
    )
    return [{key: record.get(key) for key in keys} for record in report["records"]]


def _summary(report: dict[str, Any], *, changed: dict[str, int] | None = None) -> dict[str, Any]:
    result = {key: report[key] for key in (
        "TOTAL_MISSING_SEASON", "SAFE_INFER", "CONFLICT", "NO_EVIDENCE", "NEEDS_REVIEW", "by_task_status",
    )}
    result["changed"] = changed or {"resource_season": 0, "task_payload_season": 0}
    return result


async def run(*, apply: bool, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    async with AsyncSessionLocal() as db:
        before = await audit_missing_seasons(db)
        (output_dir / "backfill_season_before.json").write_text(
            json.dumps({"summary": _summary(before), "records": _projection(before)}, ensure_ascii=False, indent=2) + "\n"
        )
        if not apply:
            return {"mode": "dry-run", **_summary(before)}
        await db.rollback()
        async with db.begin():
            changed = await apply_safe_backfill(db, before["records"])
        after = await audit_missing_seasons(db)
    after_payload = {
        "summary": _summary(after, changed=changed),
        "records": _projection(after),
        "invariant": {
            "only_season_fields_requested": True,
            "existing_season_overwrites": 0,
            "queue_claims": 0,
            "task_executions": 0,
        },
    }
    (output_dir / "backfill_season_after.json").write_text(json.dumps(after_payload, ensure_ascii=False, indent=2) + "\n")
    return {"mode": "apply", **_summary(after, changed=changed)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Safe historic season backfill")
    parser.add_argument("--apply", action="store_true", help="apply only freshly audited SAFE_INFER rows")
    parser.add_argument("--confirm-safe-infer", action="store_true", help="required with --apply")
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    if args.apply and not args.confirm_safe_infer:
        parser.error("--apply requires --confirm-safe-infer")
    print(json.dumps(asyncio.run(run(apply=args.apply, output_dir=args.output_dir)), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
