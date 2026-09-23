"""Idempotently reconcile only legacy-collected episodes verified in cloud inventory.

Default mode is a read-only dry run. ``--apply`` performs narrow set-union
updates on the matching ``SeriesWatchlist.collected_episodes`` rows; it never
replaces an entire collected array and never calls a cloud adapter.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.audit_queue_safety import load_legacy_collected, normalize_episode_key


def _as_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _sort_episode_key(value: str) -> tuple[int, int]:
    matched = re.fullmatch(r"S(\d+)E(\d+)", value)
    return (int(matched.group(1)), int(matched.group(2))) if matched else (9999, 9999)


def _canonical_values(season: int, values: object) -> set[str]:
    return {key for value in _as_list(values) if (key := normalize_episode_key(season, value))}


async def reconcile(database_url: str, legacy_watchlist: str, *, apply: bool = False) -> dict[str, Any]:
    legacy = load_legacy_collected(legacy_watchlist)
    engine = create_async_engine(database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                if not apply:
                    await connection.execute(text("SET TRANSACTION READ ONLY"))
                watch_rows = (await connection.execute(text("""
                    SELECT id, tmdb_id, title, season, collected_episodes
                    FROM series_watchlist
                    WHERE tmdb_id IS NOT NULL AND tmdb_id > 0
                    ORDER BY id
                """))).mappings().all()
                inventory_rows = (await connection.execute(text("""
                    SELECT tmdb_id, season, episode
                    FROM cloud_disk_inventory
                    WHERE tmdb_id IS NOT NULL AND season IS NOT NULL AND episode IS NOT NULL
                """))).mappings().all()
                inventory = {
                    (int(row["tmdb_id"]), int(row["season"]), key)
                    for row in inventory_rows
                    if (key := normalize_episode_key(int(row["season"]), row["episode"]))
                }
                current: dict[tuple[int, int], set[str]] = defaultdict(set)
                for row in watch_rows:
                    current[(int(row["tmdb_id"]), int(row["season"]))].update(
                        _canonical_values(int(row["season"]), row["collected_episodes"])
                    )

                candidates: set[tuple[int, int, str]] = set()
                old_db_only: set[tuple[int, int, str]] = set()
                for identity, old_episodes in legacy.items():
                    tmdb_id, season = identity
                    for episode in old_episodes:
                        if episode in current.get(identity, set()):
                            continue
                        triple = (tmdb_id, season, episode)
                        if triple in inventory:
                            candidates.add(triple)
                        else:
                            old_db_only.add(triple)

                entries: list[dict[str, Any]] = []
                updates: list[tuple[dict[str, Any], list[str], str]] = []
                for row in watch_rows:
                    tmdb_id, season = int(row["tmdb_id"]), int(row["season"])
                    existing = _canonical_values(season, row["collected_episodes"])
                    additions = sorted(
                        {episode for candidate_tmdb, candidate_season, episode in candidates
                         if (candidate_tmdb, candidate_season) == (tmdb_id, season)} - existing,
                        key=_sort_episode_key,
                    )
                    if additions:
                        new_values = sorted(existing | set(additions), key=_sort_episode_key)
                        updates.append((dict(row), new_values, json.dumps(new_values, ensure_ascii=False)))
                        for episode in additions:
                            entries.append({
                                "watchlist_id": int(row["id"]),
                                "tmdb_id": tmdb_id,
                                "title": row["title"],
                                "season": season,
                                "episode": episode,
                                "old_db": True,
                                "new_db": False,
                                "cloud_inventory": True,
                                "action": "ADD_COLLECTED",
                            })
                for tmdb_id, season, episode in sorted(old_db_only, key=lambda value: (value[0], value[1], _sort_episode_key(value[2]))):
                    titles = [row["title"] for row in watch_rows if int(row["tmdb_id"]) == tmdb_id and int(row["season"]) == season]
                    entries.append({
                        "watchlist_id": None,
                        "tmdb_id": tmdb_id,
                        "title": titles[0] if titles else None,
                        "season": season,
                        "episode": episode,
                        "old_db": True,
                        "new_db": False,
                        "cloud_inventory": False,
                        "action": "NEEDS_CLOUD_REVIEW",
                    })

                applied = 0
                if apply:
                    for row, values, payload in updates:
                        result = await connection.execute(
                            text("""
                                UPDATE series_watchlist
                                SET collected_episodes = CAST(:episodes AS jsonb)
                                WHERE id = :id
                                RETURNING id, collected_episodes
                            """),
                            {"id": int(row["id"]), "episodes": payload},
                        )
                        returned = result.mappings().one()
                        actual = _canonical_values(int(row["season"]), returned["collected_episodes"])
                        expected = set(values)
                        if actual != expected:
                            raise RuntimeError(f"readback mismatch for watchlist {row['id']}")
                        applied += len(set(values) - _canonical_values(int(row["season"]), row["collected_episodes"]))
                await transaction.commit()
            except Exception:
                await transaction.rollback()
                raise
        add_entries = [entry for entry in entries if entry["action"] == "ADD_COLLECTED"]
        return {
            "mode": "apply" if apply else "dry-run",
            "read_only": not apply,
            "criteria": "old_db=true AND new_db=false AND cloud_inventory=true, exact tmdb_id+season+episode only",
            "summary": {
                "add_collected": len(add_entries),
                "needs_cloud_review": len([entry for entry in entries if entry["action"] == "NEEDS_CLOUD_REVIEW"]),
                "applied": applied,
            },
            "episodes": entries,
        }
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--legacy-watchlist", required=True)
    parser.add_argument("--apply", action="store_true", help="perform the narrow collected set-union updates")
    parser.add_argument("--output", default="-")
    args = parser.parse_args()
    report = asyncio.run(reconcile(args.database_url, args.legacy_watchlist, apply=args.apply))
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output == "-":
        print(rendered, end="")
    else:
        Path(args.output).write_text(rendered, encoding="utf-8")
        print(json.dumps({"output": args.output, "mode": report["mode"], "summary": report["summary"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
