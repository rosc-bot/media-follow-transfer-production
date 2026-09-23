"""Strict, evidence-backed season inference for legacy queue records.

This module intentionally accepts only structured evidence.  It never derives a
season from a title, filename, or an episode number without an ``Sxx`` prefix.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

_EPISODE_KEY_RE = re.compile(r"^S0*(?P<season>[1-9]\d*)E0*(?P<episode>[1-9]\d*)$", re.IGNORECASE)


@dataclass(frozen=True)
class EvidenceConflict:
    """Conflicting structured season evidence; manual review is required."""

    values: tuple[int, ...]


@dataclass(frozen=True)
class SeasonInference:
    inferred_season: int | None
    confidence: str
    evidence: list[str]
    conflict: EvidenceConflict | None = None


def parse_episode_key(value: object) -> tuple[int, int] | None:
    """Return ``(season, episode)`` only for an exact SxxExxx episode key."""
    if not isinstance(value, str):
        return None
    match = _EPISODE_KEY_RE.fullmatch(value.strip())
    if match is None:
        return None
    return int(match["season"]), int(match["episode"])


def _normalized(values: Iterable[object] | None) -> set[int]:
    normalized: set[int] = set()
    for value in values or set():
        try:
            parsed = int(str(value))
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            normalized.add(parsed)
    return normalized


def infer_season(
    *,
    episode_key: object,
    watchlist_seasons: Iterable[object] | None = None,
    candidate_seasons: Iterable[object] | None = None,
    resource_seasons: Iterable[object] | None = None,
    payload_seasons: Iterable[object] | None = None,
) -> SeasonInference:
    """Infer only from a strict key plus non-conflicting structured evidence.

    The Sxx part is A-grade evidence.  Unique matching watchlist/candidate
    seasons strengthen the audit trail; any disagreement fails closed.
    """
    parsed = parse_episode_key(episode_key)
    if parsed is None:
        return SeasonInference(None, "NEEDS_REVIEW", ["episode_key:unparseable"])

    season, _episode = parsed
    evidence = [f"episode_key:{str(episode_key).strip().upper()}"]
    supplied: set[int] = {season}
    for label, values in (
        ("resource", resource_seasons),
        ("payload", payload_seasons),
        ("watchlist", watchlist_seasons),
        ("candidate", candidate_seasons),
    ):
        normalized = _normalized(values)
        if len(normalized) == 1:
            value = next(iter(normalized))
            supplied.add(value)
            evidence.append(f"{label}:season={value}")
        elif len(normalized) > 1:
            # C-grade watchlist evidence is valid only when it identifies one
            # season.  A multi-season followlist is neither corroboration nor a
            # conflict against structured A/B evidence.
            evidence.append(f"{label}:ambiguous={','.join(map(str, sorted(normalized)))}")

    if len(supplied) != 1:
        return SeasonInference(
            None,
            "NEEDS_REVIEW",
            evidence,
            EvidenceConflict(tuple(sorted(supplied))),
        )
    return SeasonInference(season, "SAFE_INFER", evidence)
