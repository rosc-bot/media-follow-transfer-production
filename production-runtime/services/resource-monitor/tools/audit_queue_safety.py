"""Read-only safety classification for transfer queue recovery.

The tool opens PostgreSQL in a read-only transaction and never calls a transfer
provider.  It only reads the requested queue states plus watchlist, inventory,
and an optional gzip-compressed legacy watchlist SQLite backup.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

TARGET_STATUSES = ("QUEUED", "RETRY_WAIT", "FAILED")
ACTIVE_STATUSES = ("QUEUED", "RETRY_WAIT", "RUNNING")
COMPLETED_STATUSES = ("COMPLETED",)


def normalize_episode_key(season: int | None, episode: object) -> str | None:
    """Canonicalize a legacy episode number or SxxExx token without guessing."""
    if episode is None:
        return None
    if isinstance(episode, int) or (isinstance(episode, str) and episode.strip().isdigit()):
        return f"S{int(season or 1):02d}E{int(episode):02d}"
    token = str(episode).strip().upper()
    match = re.fullmatch(r"S?(\d{1,3})E(\d{1,4})", token)
    if match:
        return f"S{int(match.group(1)):02d}E{int(match.group(2)):02d}"
    return None


def season_from_episode_key(episode_key: str | None) -> int | None:
    match = re.fullmatch(r"S(\d{2,3})E\d{2,4}", str(episode_key or ""))
    return int(match.group(1)) if match else None


def parse_episode_keys(season: int | None, resource_episode: object, payload: object) -> list[str]:
    keys: list[str] = []
    if isinstance(payload, dict):
        raw_keys = payload.get("episode_keys") or payload.get("episodes") or []
        if not isinstance(raw_keys, list):
            raw_keys = [raw_keys]
        for raw in raw_keys:
            key = normalize_episode_key(season, raw)
            if key and key not in keys:
                keys.append(key)
    resource_key = normalize_episode_key(season, resource_episode)
    if resource_key and resource_key not in keys:
        keys.append(resource_key)
    return keys


def error_type(error: str | None) -> str | None:
    if not error:
        return None
    lowered = error.lower()
    if "401" in lowered or "unauthorized" in lowered:
        return "AUTH_401"
    if "readback" in lowered or "did not verify" in lowered:
        return "READBACK_UNVERIFIED"
    if "timeout" in lowered:
        return "TIMEOUT"
    if "needs_review" in lowered or "needs review" in lowered:
        return "NEEDS_REVIEW"
    if "duplicate" in lowered or "unique constraint" in lowered:
        return "DUPLICATE"
    return "OTHER"


def classify_task(*, old_collected: bool, new_collected: bool, in_cloud: bool,
                  completed_task: bool, duplicate_idempotency: bool,
                  active_same_episode: bool, error_type: str | None) -> list[str]:
    """Return all applicable conservative labels, SAFE_NEW only if none apply."""
    labels: list[str] = []
    if old_collected:
        labels.append("ALREADY_COLLECTED_OLD")
    if new_collected:
        labels.append("ALREADY_COLLECTED_NEW")
    if in_cloud:
        labels.append("ALREADY_IN_CLOUD")
    if completed_task:
        labels.append("POSSIBLE_DUPLICATE")
    if duplicate_idempotency or active_same_episode:
        labels.append("DUPLICATE_TASK")
    if error_type == "AUTH_401":
        labels.append("AUTH_BLOCKED")
    elif error_type is not None:
        labels.append("NEEDS_REVIEW")
    return labels or ["SAFE_NEW"]


def _json_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def load_legacy_collected(path: str | None) -> dict[tuple[int, int], set[str]]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(source)
    raw = gzip.decompress(source.read_bytes()) if source.suffix == ".gz" else source.read_bytes()
    # In-memory only: no extracted database file is created.
    db = sqlite3.connect(":memory:")
    try:
        db.deserialize(raw)
        columns = {row[1] for row in db.execute("PRAGMA table_info(watchlist)")}
        required = {"tmdb_id", "season", "collected_episodes"}
        if not required.issubset(columns):
            raise ValueError(f"legacy watchlist missing columns: {sorted(required - columns)}")
        result: dict[tuple[int, int], set[str]] = defaultdict(set)
        for tmdb_id, season, episodes in db.execute(
            "SELECT tmdb_id, season, collected_episodes FROM watchlist WHERE tmdb_id IS NOT NULL"
        ):
            for episode in _json_list(episodes):
                key = normalize_episode_key(season, episode)
                if key:
                    result[(int(tmdb_id), int(season or 1))].add(key)
        return dict(result)
    finally:
        db.close()


async def audit(database_url: str, legacy_watchlist: str | None) -> dict[str, Any]:
    legacy = load_legacy_collected(legacy_watchlist)
    engine = create_async_engine(database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn, conn.begin():
                await conn.execute(text("SET TRANSACTION READ ONLY"))
                tasks = (await conn.execute(text("""
                    SELECT q.id AS task_id, q.idempotency_key, q.status, q.attempt_count,
                           q.max_retries, q.error_message, q.payload, q.created_at,
                           r.id AS resource_id, r.title, r.tmdb_id, r.season,
                           r.episode, r.episode_key, r.share_url, r.source_type, r.status AS resource_status
                    FROM transfer_queue_tasks q
                    LEFT JOIN resources r ON r.id = q.resource_id
                    WHERE q.status = ANY(:statuses)
                    ORDER BY q.id
                """), {"statuses": list(TARGET_STATUSES)})).mappings().all()
                watchlists = (await conn.execute(text("""
                    SELECT tmdb_id, season, collected_episodes
                    FROM series_watchlist
                """))).mappings().all()
                inventory = (await conn.execute(text("""
                    SELECT tmdb_id, season, episode
                    FROM cloud_disk_inventory
                    WHERE tmdb_id IS NOT NULL
                """))).mappings().all()
                all_queue = (await conn.execute(text("""
                    SELECT q.id, q.idempotency_key, q.status, r.tmdb_id, r.season, r.episode, r.episode_key, q.payload
                    FROM transfer_queue_tasks q
                    JOIN resources r ON r.id = q.resource_id
                """))).mappings().all()

        new_collected: dict[tuple[int, int], set[str]] = defaultdict(set)
        for row in watchlists:
            for ep in _json_list(row["collected_episodes"]):
                key = normalize_episode_key(row["season"], ep)
                if key:
                    new_collected[(int(row["tmdb_id"]), int(row["season"]))].add(key)
        cloud = {
            (int(row["tmdb_id"]), int(row["season"]), normalize_episode_key(row["season"], row["episode"]))
            for row in inventory
            if normalize_episode_key(row["season"], row["episode"])
        }
        completed_keys: set[tuple[int, int, str]] = set()
        active_counts: Counter[tuple[int, int, str]] = Counter()
        idempotency_counts: Counter[str] = Counter()
        for row in all_queue:
            idempotency_counts[row["idempotency_key"]] += 1
            if row["tmdb_id"] is None or row["season"] is None:
                continue
            for ep in parse_episode_keys(row["season"], row["episode_key"] or row["episode"], row["payload"]):
                episode_season = int(row["season"]) if row["season"] is not None else season_from_episode_key(ep)
                if episode_season is None:
                    continue
                key = (int(row["tmdb_id"]), episode_season, ep)
                if row["status"] in COMPLETED_STATUSES:
                    completed_keys.add(key)
                if row["status"] in ACTIVE_STATUSES:
                    active_counts[key] += 1

        output: list[dict[str, Any]] = []
        for row in tasks:
            tmdb_id = int(row["tmdb_id"]) if row["tmdb_id"] is not None else None
            stored_season = int(row["season"]) if row["season"] is not None else None
            episode_keys = parse_episode_keys(stored_season, row["episode_key"] or row["episode"], row["payload"])
            inferred_seasons = {season_from_episode_key(ep) for ep in episode_keys if season_from_episode_key(ep) is not None}
            season = stored_season if stored_season is not None else (next(iter(inferred_seasons)) if len(inferred_seasons) == 1 else None)
            checks = []
            all_labels: set[str] = set()
            for episode_key in episode_keys or [None]:
                episode_season = stored_season if stored_season is not None else season_from_episode_key(episode_key)
                identity = (tmdb_id, episode_season, episode_key) if tmdb_id is not None and episode_season is not None and episode_key else None
                old_hit = bool(identity and episode_key in legacy.get((tmdb_id, episode_season), set()))
                new_hit = bool(identity and episode_key in new_collected.get((tmdb_id, episode_season), set()))
                cloud_hit = bool(identity and identity in cloud)
                completed_hit = bool(identity and identity in completed_keys)
                active_same = bool(identity and active_counts[identity] > 1)
                labels = classify_task(
                    old_collected=old_hit, new_collected=new_hit, in_cloud=cloud_hit,
                    completed_task=completed_hit,
                    duplicate_idempotency=idempotency_counts[row["idempotency_key"]] > 1,
                    active_same_episode=active_same,
                    error_type=error_type(row["error_message"]),
                )
                checks.append({
                    "season": episode_season,
                    "episode": episode_key,
                    "old_collected": old_hit,
                    "new_collected": new_hit,
                    "cloud_inventory": cloud_hit,
                    "completed_task": completed_hit,
                    "active_same_episode": active_same,
                    "classification": labels,
                })
                all_labels.update(labels)
            if not episode_keys:
                all_labels.add("NEEDS_REVIEW")
            output.append({
                "task_id": row["task_id"], "title": row["title"], "tmdb_id": tmdb_id,
                "season": season, "episode": episode_keys, "status": row["status"],
                "share_url": row["share_url"], "source_type": row["source_type"],
                "attempt_count": row["attempt_count"], "error_type": error_type(row["error_message"]),
                "error_message": row["error_message"], "resource_id": row["resource_id"],
                "resource_status": row["resource_status"],
                "idempotency_key": row["idempotency_key"], "classification": sorted(all_labels),
                "checks": checks,
            })
        return {
            "read_only": True,
            "target_statuses": list(TARGET_STATUSES),
            "summary_by_status": dict(Counter(item["status"] for item in output)),
            "summary_by_label": dict(Counter(label for item in output for label in item["classification"])),
            "tasks": output,
        }
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--legacy-watchlist", help="SQLite or .db.gz path; read in memory only")
    parser.add_argument("--output", default="-", help="JSON file path, or - for stdout")
    args = parser.parse_args()
    report = asyncio.run(audit(args.database_url, args.legacy_watchlist))
    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.output == "-":
        print(payload)
    else:
        Path(args.output).write_text(payload)
        print(json.dumps({"output": args.output, "tasks": len(report["tasks"]), "read_only": True}))


if __name__ == "__main__":
    main()
