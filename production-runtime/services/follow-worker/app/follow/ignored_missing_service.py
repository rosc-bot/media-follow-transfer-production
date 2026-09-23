"""One shared representation of ignored-missing rules for Radar and workers."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ignored_missing import IgnoredMissing

IgnoreIndex = Mapping[tuple[str, int], frozenset[int]]


def normalize_ignored_title(title: object) -> str:
    return str(title or "").strip().casefold()


class IgnoredMissingService:
    @staticmethod
    async def load_index(db: AsyncSession) -> IgnoreIndex:
        grouped: defaultdict[tuple[str, int], set[int]] = defaultdict(set)
        rows = (await db.scalars(select(IgnoredMissing))).all()
        for row in rows:
            grouped[(normalize_ignored_title(row.title), int(row.season))].add(int(row.episode))
        return {key: frozenset(episodes) for key, episodes in grouped.items()}

    @staticmethod
    def episodes_for(watchlist: object, index: IgnoreIndex) -> frozenset[int]:
        return index.get(
            (normalize_ignored_title(getattr(watchlist, "title", "")), int(getattr(watchlist, "season", 1) or 1)),
            frozenset(),
        )
