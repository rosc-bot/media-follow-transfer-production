"""Canonical persisted follow-mode compatibility semantics."""

from __future__ import annotations

FULL = "FULL"
LATEST = "LATEST"


def normalize_follow_mode(value: object) -> str:
    """Map legacy values without rewriting existing database rows.

    ``ALL`` historically meant complete catch-up; ``AUTO`` historically acted
    as the normal recent-follow path. Unknown/null values deliberately fail to
    the conservative recent-only mode rather than triggering a large backlog.
    """
    mode = str(value or "").strip().upper()
    if mode in {FULL, "ALL"}:
        return FULL
    return LATEST
