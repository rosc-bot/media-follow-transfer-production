"""Cancel one exact QUEUED task only after current collection + cloud confirmation.

This is deliberately task-id scoped and adapter-free. It never reads or changes
FAILED/RETRY_WAIT history and defaults to a read-only dry run.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

_EPISODE = re.compile(r"^S(\d{1,3})E(\d{1,4})$")
_REASON = "RECONCILED_ALREADY_IN_CLOUD"


def _episode_key(value: object) -> tuple[int, int, str] | None:
    if not isinstance(value, str):
        return None
    match = _EPISODE.fullmatch(value.strip().upper())
    if not match:
        return None
    season, episode = int(match.group(1)), int(match.group(2))
    return season, episode, f"S{season:02d}E{episode:02d}"


def _payload_key(payload: object) -> tuple[int, int, str] | None:
    if not isinstance(payload, dict):
        return None
    keys = payload.get("episode_keys") or []
    if not isinstance(keys, list) or len(keys) != 1:
        return None
    return _episode_key(keys[0])


async def cancel_if_reconciled(database_url: str, task_id: int, *, apply: bool = False) -> dict:
    engine = create_async_engine(database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                if not apply:
                    await connection.execute(text("SET TRANSACTION READ ONLY"))
                task = (await connection.execute(text("""
                    SELECT q.id, q.status, q.resource_id, q.payload,
                           r.tmdb_id, r.season AS resource_season, r.episode_key, r.title
                    FROM transfer_queue_tasks q
                    LEFT JOIN resources r ON r.id = q.resource_id
                    WHERE q.id = :task_id
                """), {"task_id": task_id})).mappings().one_or_none()
                if task is None:
                    report = {"mode": "apply" if apply else "dry-run", "task_id": task_id, "decision": "TASK_NOT_FOUND"}
                else:
                    key = _payload_key(task["payload"]) or _episode_key(task["episode_key"])
                    tmdb_id = int(task["tmdb_id"]) if task["tmdb_id"] is not None else None
                    if key is None or tmdb_id is None or tmdb_id <= 0:
                        report = {"mode": "apply" if apply else "dry-run", "task_id": task_id, "before_status": task["status"], "decision": "UNVERIFIABLE_IDENTITY"}
                    else:
                        season, episode, canonical = key
                        resource_season = task["resource_season"]
                        if resource_season is not None and int(resource_season) != season:
                            report = {"mode": "apply" if apply else "dry-run", "task_id": task_id, "before_status": task["status"], "decision": "IDENTITY_MISMATCH"}
                        else:
                            watch = (await connection.execute(text("""
                                SELECT collected_episodes FROM series_watchlist
                                WHERE tmdb_id = :tmdb_id AND season = :season
                            """), {"tmdb_id": tmdb_id, "season": season})).mappings().all()
                            collected = {
                                str(value).strip().upper()
                                for row in watch
                                for value in (row["collected_episodes"] if isinstance(row["collected_episodes"], list) else [])
                            }
                            in_watchlist = canonical in collected
                            in_cloud = (await connection.execute(text("""
                                SELECT EXISTS(
                                    SELECT 1 FROM cloud_disk_inventory
                                    WHERE tmdb_id = :tmdb_id AND season = :season AND episode = :episode
                                )
                            """), {"tmdb_id": tmdb_id, "season": season, "episode": episode})).scalar_one()
                            eligible = task["status"] == "QUEUED" and in_watchlist and bool(in_cloud)
                            report = {
                                "mode": "apply" if apply else "dry-run",
                                "task_id": task_id,
                                "title": task["title"],
                                "before_status": task["status"],
                                "identity": {"tmdb_id": tmdb_id, "season": season, "episode": canonical},
                                "evidence": {"new_collected": in_watchlist, "cloud_inventory": bool(in_cloud)},
                                "decision": "CANCEL_ELIGIBLE" if eligible else "KEEP_UNCHANGED",
                                "reason": _REASON if eligible else None,
                            }
                            if apply and eligible:
                                changed = (await connection.execute(text("""
                                    UPDATE transfer_queue_tasks
                                    SET status = 'CANCELLED', locked_at = NULL, locked_by = NULL,
                                        error_message = :reason
                                    WHERE id = :task_id AND status = 'QUEUED'
                                    RETURNING status, locked_at, locked_by, error_message
                                """), {"task_id": task_id, "reason": _REASON})).mappings().one_or_none()
                                if changed is None:
                                    raise RuntimeError("queue status changed before guarded cancellation")
                                if changed["status"] != "CANCELLED" or changed["locked_at"] is not None or changed["locked_by"] is not None:
                                    raise RuntimeError("queue cancellation readback failed")
                                report["after_status"] = changed["status"]
                                report["applied"] = True
                await transaction.commit()
                return report
            except Exception:
                await transaction.rollback()
                raise
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--task-id", required=True, type=int)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output", default="-")
    args = parser.parse_args()
    report = asyncio.run(cancel_if_reconciled(args.database_url, args.task_id, apply=args.apply))
    body = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output == "-":
        print(body, end="")
    else:
        Path(args.output).write_text(body, encoding="utf-8")
        print(json.dumps({"output": args.output, "decision": report["decision"], "mode": report["mode"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
