"""Fail-closed promotion readiness and readback decisions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.follow.episode_keys import canonical_episode_key

_ENDED = frozenset({"ended", "canceled", "cancelled"})


@dataclass(frozen=True)
class PromotionDecision:
    decision: str
    tmdb_id: int
    title: str
    series_status: str | None
    seasons: tuple[dict[str, Any], ...]
    total_expected: int
    collected_count: int
    inventory_count: int
    cloud_count: int
    missing_count: int
    active_transfer_count: int
    ongoing_root: str | None
    completed_root: str | None
    destination_conflict: bool = False
    reason: str | None = None
    expected_files_by_season: dict[str, tuple[str, ...]] = field(default_factory=dict)
    promotion_evaluation_id: str | None = None
    cloud_scan_status: str | None = None
    cloud_scan_timestamp: str | None = None
    cloud_scan_watermark: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "tmdb_id": self.tmdb_id,
            "title": self.title,
            "series_status": self.series_status,
            "seasons": [dict(item) for item in self.seasons],
            "total_expected": self.total_expected,
            "collected_count": self.collected_count,
            "inventory_count": self.inventory_count,
            "cloud_count": self.cloud_count,
            "missing_count": self.missing_count,
            "active_transfer_count": self.active_transfer_count,
            "ongoing_root": self.ongoing_root,
            "completed_root": self.completed_root,
            "destination_conflict": self.destination_conflict,
            "reason": self.reason,
            "expected_files_by_season": {
                season: list(files) for season, files in self.expected_files_by_season.items()
            },
            "promotion_evaluation_id": self.promotion_evaluation_id,
            "cloud_scan_status": self.cloud_scan_status,
            "cloud_scan_timestamp": self.cloud_scan_timestamp,
            "cloud_scan_watermark": self.cloud_scan_watermark,
        }


def _status(value: object) -> str:
    return str(value or "").strip().casefold()


def _count(season: dict[str, Any], key: str) -> int:
    try:
        return max(0, int(season.get(key) or 0))
    except (TypeError, ValueError):
        return 0


def evaluate_promotion(
    *,
    tmdb_id: int,
    title: str,
    series_status: str | None,
    seasons: list[dict[str, Any]],
    ongoing_root: str | None,
    completed_root: str | None,
    active_transfer_count: int = 0,
    destination_conflict: bool = False,
    expected_files_by_season: dict[str, list[str]] | None = None,
    promotion_evaluation_id: str | None = None,
    cloud_scan_status: str | None = None,
    cloud_scan_timestamp: str | None = None,
    cloud_scan_watermark: str | None = None,
    require_cloud_scan_watermark: bool = False,
) -> PromotionDecision:
    """Evaluate all gates without contacting or mutating a provider.

    ``cloud_count`` is physical authenticated listing evidence, not the incoming
    transfer batch size.  A missing/zero value consequently fails closed.
    """

    normalized_seasons = tuple(dict(item) for item in seasons)
    total_expected = sum(_count(item, "total_expected") for item in normalized_seasons)
    collected_count = sum(_count(item, "collected_count") for item in normalized_seasons)
    inventory_count = sum(_count(item, "inventory_count") for item in normalized_seasons)
    cloud_count = sum(_count(item, "cloud_count") for item in normalized_seasons)
    missing_count = max(
        0,
        total_expected - collected_count,
        total_expected - inventory_count,
        total_expected - cloud_count,
    )
    active = max(0, int(active_transfer_count or 0))
    is_multi = len(normalized_seasons) > 1
    root_status = _status(series_status)
    scan_status = str(cloud_scan_status or "VERIFIED").strip().upper()
    season_not_ended = [item for item in normalized_seasons if _status(item.get("series_status") or series_status) not in _ENDED]
    collected_key_gap = any(
        set(item.get("expected_episode_keys") or ())
        and not set(item.get("expected_episode_keys") or ()).issubset(set(item.get("collected_episode_keys") or ()))
        for item in normalized_seasons
    )
    inventory_key_gap = any(
        set(item.get("expected_episode_keys") or ())
        and not set(item.get("expected_episode_keys") or ()).issubset(set(item.get("inventory_episode_keys") or ()))
        for item in normalized_seasons
    )
    cloud_key_gap = any(
        set(item.get("expected_episode_keys") or ())
        and not set(item.get("expected_episode_keys") or ()).issubset(set(item.get("cloud_episode_keys") or ()))
        for item in normalized_seasons
    )

    if not ongoing_root or not completed_root:
        decision, reason = "NEEDS_REVIEW", "DESTINATION_ROOT_MISSING"
    elif season_not_ended or root_status not in _ENDED:
        decision, reason = ("SERIES_NOT_READY" if is_multi else "SERIES_NOT_ENDED"), "AUTHORITATIVE_STATUS_NOT_ENDED"
    elif active:
        decision, reason = "ACTIVE_TRANSFER", "ACTIVE_TRANSFER_TASK_EXISTS"
    elif collected_count < total_expected or collected_key_gap:
        decision, reason = "CONTENT_INCOMPLETE", "COLLECTED_EPISODES_INCOMPLETE"
    elif inventory_count < total_expected or inventory_key_gap:
        decision, reason = "INVENTORY_INCOMPLETE", "INVENTORY_EPISODES_INCOMPLETE"
    elif scan_status != "VERIFIED" or (require_cloud_scan_watermark and not cloud_scan_watermark):
        decision, reason = "SCAN_UNVERIFIED", "PHYSICAL_CLOUD_SCAN_NOT_VERIFIED"
    elif cloud_count < total_expected or cloud_key_gap:
        decision, reason = "CLOUD_INCOMPLETE", "PHYSICAL_CLOUD_EPISODES_INCOMPLETE"
    elif destination_conflict:
        decision, reason = "DESTINATION_CONFLICT", "COMPLETED_SERIES_ROOT_ALREADY_EXISTS"
    else:
        decision, reason = "PROMOTION_READY", None

    expected = {
        str(season): tuple(sorted({str(name).strip() for name in files if str(name).strip()}))
        for season, files in (expected_files_by_season or {}).items()
    }
    return PromotionDecision(
        decision=decision,
        tmdb_id=int(tmdb_id),
        title=str(title or "").strip(),
        series_status=series_status,
        seasons=normalized_seasons,
        total_expected=total_expected,
        collected_count=collected_count,
        inventory_count=inventory_count,
        cloud_count=cloud_count,
        missing_count=missing_count,
        active_transfer_count=active,
        ongoing_root=str(ongoing_root or "") or None,
        completed_root=str(completed_root or "") or None,
        destination_conflict=bool(destination_conflict),
        reason=reason,
        expected_files_by_season=expected,
        promotion_evaluation_id=promotion_evaluation_id,
        cloud_scan_status=scan_status,
        cloud_scan_timestamp=cloud_scan_timestamp,
        cloud_scan_watermark=cloud_scan_watermark,
    )


def promotion_readback_decision(
    *,
    move_returned: bool,
    expected_files_by_season: dict[str, list[str]],
    observed_files_by_season: dict[str, list[str]],
    source_root_exists: bool,
    destination_conflict: bool,
    destination_root_exists: bool = True,
) -> str:
    """Classify a move only after destination and source readback."""

    if not move_returned or destination_conflict or source_root_exists or not destination_root_exists:
        return "PROMOTION_UNVERIFIED"
    for season, expected in expected_files_by_season.items():
        expected_set = {str(name).strip().casefold() for name in expected if str(name).strip()}
        observed_set = {
            str(name).strip().casefold()
            for name in observed_files_by_season.get(str(season), [])
            if str(name).strip()
        }
        if not expected_set.issubset(observed_set):
            return "PROMOTION_UNVERIFIED"
    return "PROMOTION_COMPLETED"


def canonical_keys(values: list[Any] | None, season: int) -> set[str]:
    return {
        key
        for value in values or []
        if (key := canonical_episode_key(season, value)) is not None
    }


__all__ = ["PromotionDecision", "canonical_keys", "evaluate_promotion", "promotion_readback_decision"]
