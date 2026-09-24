"""Canonical season/episode keys used at every persisted boundary."""

from __future__ import annotations

import re
from collections.abc import Iterable

_FULL_KEY = re.compile(r"^S?(?P<season>\d{1,3})E(?P<episode>\d{1,4})$", re.IGNORECASE)
_EPISODE_ONLY = re.compile(r"^E?(?P<episode>\d{1,4})$", re.IGNORECASE)
_SEASON_S = re.compile(r"^S\s*0*(\d+)$", re.IGNORECASE)
_SEASON_EN = re.compile(r"^SEASON\s*0*(\d+)$", re.IGNORECASE)
_SEASON_AR = re.compile(r"^第\s*0*(\d+)\s*季$")
_SEASON_ZH = re.compile(r"^第([一二三四五六七八九十]+)季$")
_ZH_DIGITS = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}


def _chinese_season_number(value: str) -> int | None:
    if value == "十":
        return 10
    if value.startswith("十"):
        tail = value[1:]
        return 10 + _ZH_DIGITS[tail] if tail in _ZH_DIGITS else None
    if "十" in value:
        tens, ones = value.split("十", 1)
        if tens in _ZH_DIGITS and (not ones or ones in _ZH_DIGITS):
            return _ZH_DIGITS[tens] * 10 + (_ZH_DIGITS.get(ones, 0))
        return None
    return _ZH_DIGITS.get(value)


def season_identity(name: object) -> int | None:
    """Return a season number only when ``name`` is an explicit season label.

    Exact forms such as ``S02``, ``Season 2`` and ``第二季`` share one numeric
    identity. Extra words or multiple season tokens are deliberately rejected.
    """
    value = str(name or "").strip()
    if not value:
        return None
    for pattern in (_SEASON_S, _SEASON_EN, _SEASON_AR):
        match = pattern.fullmatch(value)
        if match:
            number = int(match.group(1))
            return number if number > 0 else None
    match = _SEASON_ZH.fullmatch(value)
    if match:
        number = _chinese_season_number(match.group(1))
        return number if number and number > 0 else None
    return None


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
