"""EnhancedRadarService — three-layer missing-episode radar with TMDB air-date gating.

Fuses three data sources:
    1. **SeriesWatchlist** — the user's tracked series (TMDB metadata, expected episodes).
    2. **Emby library** — what episodes actually exist on the media server.
    3. **TMDB season details** — per-episode air dates to gate future/unaired episodes.

Key behaviours migrated from the old bot's ``LibraryService.get_radar_summary()``:
    • Episodes whose TMDB ``air_date`` is in the future are **excluded** from the
      missing count (they haven't aired yet).
    • Missing episodes are classified into three tiers:
        - ``trailing_new``  — consecutive gap at the tail (latest unwatched).
        - ``mid_gaps``      — holes in the middle of the season.
        - ``early_gaps``    — missing episodes at the very start.
    • The ``follow_mode`` parameter (``LATEST`` vs ``ALL``) controls whether only
      the newest season or every season is scanned.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.follow.follow_mode import LATEST, normalize_follow_mode
from app.follow.ignored_missing_service import IgnoredMissingService, normalize_ignored_title

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data containers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class MissingBreakdown:
    """Three-tier classification of missing episodes for a single season."""
    trailing_new: list[int] = field(default_factory=list)
    mid_gaps: list[int] = field(default_factory=list)
    early_gaps: list[int] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.trailing_new) + len(self.mid_gaps) + len(self.early_gaps)

    def to_dict(self) -> dict[str, Any]:
        return {
            "trailing_new": self.trailing_new,
            "mid_gaps": self.mid_gaps,
            "early_gaps": self.early_gaps,
            "total": self.total,
        }


@dataclass
class SeasonRadar:
    """Radar result for one season of a series."""
    title: str
    tmdb_id: int
    season: int
    total_episodes: int
    library_episodes: list[int]
    missing: MissingBreakdown
    aired_episodes: list[int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "tmdb_id": self.tmdb_id,
            "season": self.season,
            "total_episodes": self.total_episodes,
            "library_count": len(self.library_episodes),
            "aired_count": len(self.aired_episodes),
            "missing": self.missing.to_dict(),
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Service
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class EnhancedRadarService:
    """Stateless service — every public method is ``@staticmethod async``."""

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    @staticmethod
    async def build_enhanced(
        db: AsyncSession,
        *,
        watchlist_items: list[Any],
        library_lookup: dict[str, dict[int, list[int]]],
        tmdb_season_fetcher: Any,  # async (tmdb_id, season) -> list[dict]
        follow_mode: str = "LATEST",
    ) -> dict[str, Any]:
        """Build the full radar summary.

        Parameters
        ----------
        db:
            Async database session (used to query ``ignored_missing``).
        watchlist_items:
            List of ``SeriesWatchlist`` rows. Each must expose ``.tmdb_id``,
            ``.title``, ``.seasons`` (``dict[int, int]`` mapping season num →
            total episode count).
        library_lookup:
            ``{ title_normalised: { season_num: [ep_nums…] } }`` — what Emby
            currently has.  Built by the caller from the Emby library scan.
        tmdb_season_fetcher:
            ``async def fetch(tmdb_id: int, season: int) -> list[dict]`` where
            each dict has at least ``{"episode_number": int, "air_date": "YYYY-MM-DD" | None}``.
        follow_mode:
            ``"LATEST"`` — only scan the latest season per series.
            ``"ALL"``    — scan every season.

        Returns
        -------
        dict with keys:
            ``total_library_tasks``, ``airing_today_matches``,
            ``missing_in_library``, ``completed_in_library``
        """
        today = date.today()

        # Pre-load all ignored entries once; both Radar implementations use the
        # same normalized title/season/episode rule.
        ignored_index = await IgnoredMissingService.load_index(db)

        results: list[SeasonRadar] = []
        airing_today: list[dict[str, Any]] = []

        for item in watchlist_items:
            seasons_to_scan = EnhancedRadarService._seasons_to_scan(item, follow_mode)

            for season_num, total_eps in seasons_to_scan.items():
                # --- TMDB air-date gating ---
                try:
                    tmdb_eps = await tmdb_season_fetcher(item.tmdb_id, season_num)
                except Exception:
                    logger.warning(
                        "TMDB fetch failed for %s S%02d, falling back to no gating",
                        item.title, season_num,
                    )
                    tmdb_eps = []

                aired, today_eps = EnhancedRadarService._partition_aired(
                    tmdb_eps, today, total_eps
                )

                if today_eps:
                    airing_today.append({
                        "title": item.title,
                        "tmdb_id": item.tmdb_id,
                        "season": season_num,
                        "episodes": today_eps,
                    })

                # --- Library presence ---
                norm_title = EnhancedRadarService._normalise(item.title)
                lib_eps = library_lookup.get(norm_title, {}).get(season_num, [])

                # --- Compute missing (aired − library − ignored) ---
                ignored_eps = ignored_index.get((norm_title, season_num), frozenset())
                # episode=0 means whole season ignored
                if 0 in ignored_eps:
                    missing_eps: list[int] = []
                else:
                    missing_eps = sorted(
                        set(aired) - set(lib_eps) - set(ignored_eps)
                    )

                breakdown = EnhancedRadarService._classify_missing(
                    missing_eps, aired
                )

                results.append(SeasonRadar(
                    title=item.title,
                    tmdb_id=item.tmdb_id,
                    season=season_num,
                    total_episodes=total_eps,
                    library_episodes=sorted(lib_eps),
                    missing=breakdown,
                    aired_episodes=sorted(aired),
                ))

        missing_items = [r for r in results if r.missing.total > 0]
        completed_items = [r for r in results if r.missing.total == 0 and r.aired_episodes]

        return {
            "total_library_tasks": len(results),
            "airing_today_matches": [a for a in airing_today],
            "missing_in_library": [r.to_dict() for r in missing_items],
            "completed_in_library": [r.to_dict() for r in completed_items],
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _seasons_to_scan(item: Any, mode: str) -> dict[int, int]:
        """Given a watchlist item, decide which seasons to scan."""
        seasons: dict[int, int] = item.seasons  # {season_num: total_eps}
        if not seasons:
            return {}
        if normalize_follow_mode(mode) == LATEST:
            latest = max(seasons)
            return {latest: seasons[latest]}
        return dict(seasons)

    @staticmethod
    def _partition_aired(
        tmdb_eps: list[dict[str, Any]],
        today: date,
        total_eps: int,
    ) -> tuple[list[int], list[int]]:
        """Split TMDB episodes into (aired_so_far, airing_today).

        If *tmdb_eps* is empty (fetch failed), assumes episodes 1..total_eps
        have all aired (conservative fallback — better to flag missing than
        silently hide them).
        """
        if not tmdb_eps:
            return list(range(1, total_eps + 1)), []

        aired: list[int] = []
        airing_today: list[int] = []

        for ep in tmdb_eps:
            ep_num = ep.get("episode_number")
            raw_date = ep.get("air_date")
            if ep_num is None:
                continue

            if raw_date is None:
                # Unknown air date — treat as not yet aired (skip)
                continue

            try:
                air = (
                    datetime.strptime(raw_date, "%Y-%m-%d").date()
                    if isinstance(raw_date, str)
                    else raw_date
                )
            except (ValueError, TypeError):
                continue

            if air <= today:
                aired.append(ep_num)
            if air == today:
                airing_today.append(ep_num)

        return aired, airing_today

    @staticmethod
    def _classify_missing(
        missing: list[int], aired: list[int]
    ) -> MissingBreakdown:
        """Classify sorted *missing* episode numbers into three tiers.

        Tiers
        -----
        - **trailing_new**: consecutive run at the tail of *aired* that are
          missing — these are the newest unwatched episodes.
        - **early_gaps**: consecutive run from episode 1 that are missing.
        - **mid_gaps**: everything else (holes in the middle).
        """
        if not missing:
            return MissingBreakdown()

        max_aired = max(aired) if aired else 0
        missing_set = set(missing)

        # --- trailing: walk backwards from max_aired ---
        trailing: list[int] = []
        ep = max_aired
        while ep >= 1 and ep in missing_set:
            trailing.append(ep)
            ep -= 1
        trailing.sort()

        # --- early: walk forwards from 1 ---
        early: list[int] = []
        ep = 1
        while ep <= max_aired and ep in missing_set and ep not in trailing:
            early.append(ep)
            ep += 1

        # --- mid: everything else ---
        trailing_set = set(trailing)
        early_set = set(early)
        mid = sorted(missing_set - trailing_set - early_set)

        return MissingBreakdown(
            trailing_new=trailing,
            mid_gaps=mid,
            early_gaps=early,
        )

    @staticmethod
    def _normalise(title: str) -> str:
        return normalize_ignored_title(title)
