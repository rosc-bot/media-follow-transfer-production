"""Canonical identity extraction for transfer-task deduplication."""

from __future__ import annotations

from typing import Any

from app.follow.episode_keys import canonical_episode_key


def task_identity(payload: dict[str, Any] | None, resource: Any | None = None) -> tuple[int | None, int | None, frozenset[str]]:
    """Extract ``tmdb_id``, season and canonical episode keys from one task.

    Historical queue payloads do not always carry the identity fields, so the
    optional Resource fallback is deliberately part of this shared helper.
    """
    data = payload or {}
    raw_tmdb = data.get("tmdb_id") or getattr(resource, "tmdb_id", None)
    raw_season = data.get("season") or getattr(resource, "season", None)
    try:
        tmdb_id = int(raw_tmdb) if raw_tmdb is not None else None
    except (TypeError, ValueError):
        tmdb_id = None
    try:
        season = int(raw_season) if raw_season is not None else None
    except (TypeError, ValueError):
        season = None
    raw_keys = data.get("episode_keys")
    if not raw_keys:
        resource_key = getattr(resource, "episode_key", None)
        raw_keys = [resource_key] if resource_key else []
    if not isinstance(raw_keys, (list, tuple, set)):
        raw_keys = [raw_keys]
    if season is None:
        for raw_key in raw_keys:
            inferred = canonical_episode_key(None, raw_key)
            if inferred is not None:
                season = int(inferred.split("E", 1)[0][1:])
                break
    keys = frozenset(
        key
        for value in raw_keys
        if (key := canonical_episode_key(season, value)) is not None
    )
    return tmdb_id, season, keys


def task_matches_episode(
    payload: dict[str, Any] | None,
    *,
    tmdb_id: int,
    season: int,
    episode_keys: set[str] | frozenset[str],
    resource: Any | None = None,
) -> bool:
    current_tmdb, current_season, current_keys = task_identity(payload, resource)
    return (
        current_tmdb == int(tmdb_id)
        and current_season == int(season)
        and bool(current_keys & set(episode_keys))
    )
