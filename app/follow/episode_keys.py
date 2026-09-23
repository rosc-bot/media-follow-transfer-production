"""Canonical season/episode keys used at every persisted boundary."""

from __future__ import annotations

import re
from collections.abc import Iterable

_FULL_KEY = re.compile(r"^S?(?P<season>\d{1,3})E(?P<episode>\d{1,4})$", re.IGNORECASE)
_EPISODE_ONLY = re.compile(r"^E?(?P<episode>\d{1,4})$", re.IGNORECASE)


def canonical_episode_key(season: int | None, value: object) -> str | None:
    """Return ``SxxEyy`` without preserving legacy zero padding.

    Episode-only values require the caller's season.  A missing season falls
    back to season 1 for legacy compatibility; production callers carrying a
    verified series identity always pass the row season explicitly.
    """
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().upper().replace(" ", "")
    if not text:
        return None
    match = _FULL_KEY.fullmatch(text)
    if match:
        resolved_season = int(match.group("season"))
        episode = int(match.group("episode"))
    else:
        match = _EPISODE_ONLY.fullmatch(text)
        if not match:
            return None
        resolved_season = int(season or 1)
        episode = int(match.group("episode"))
    if resolved_season <= 0 or episode <= 0:
        return None
    return f"S{resolved_season:02d}E{episode:02d}"


def canonical_episode_keys(values: Iterable[object] | None, *, season: int | None = None) -> list[str]:
    """Normalize, deduplicate and numerically sort valid episode keys."""
    normalized = {
        key
        for value in values or []
        if (key := canonical_episode_key(season, value)) is not None
    }
    return sorted(normalized, key=episode_sort_key)


def episode_sort_key(value: str) -> tuple[int, int]:
    match = _FULL_KEY.fullmatch(str(value).strip().upper())
    if not match:
        return (10**9, 10**9)
    return int(match.group("season")), int(match.group("episode"))


def episode_number(value: object) -> int | None:
    key = canonical_episode_key(None, value)
    if not key:
        return None
    return int(key.split("E", 1)[1])
