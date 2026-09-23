"""Shared final queue preflight classification.

The classifier consumes the existing Phase 2E/2G read-only report and the
canonical destination result.  It has no provider or database side effects.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.transfer.rename import build_rename_plan

AUTO_SAFE = "AUTO_SAFE"
NEEDS_REVIEW = "NEEDS_REVIEW"
REJECTED = "REJECTED"

_TRANSIENT = frozenset({
    "NETWORK_TIMEOUT",
    "RATE_LIMITED",
    "SHARE_API_ERROR",
    "NETWORK_ERROR",
    "REMOTE_5XX",
    "ACCOUNT_AUTH_FAILED",
    "AUTH_EXPIRED",
})
_REQUIRED_CHECKS = frozenset({
    "check_task_status",
    "check_resource",
    "check_watchlist",
    "check_collected",
    "check_cloud_inventory",
    "check_duplicate_success",
    "check_duplicate_active",
    "check_share_url",
    "check_share_access",
    "check_video_files",
    "check_episode_match",
    "check_destination",
    "check_account_auth",
    "check_candidate_status",
    "check_source_type",
})


def build_source_rename_plan(
    report: Mapping[str, Any],
    *,
    payload: Mapping[str, Any],
    resource: Any,
) -> dict[str, Any]:
    """Plan selected-source naming from the same read-only share evidence.

    This is intentionally a pure operation.  The adapter still creates its
    definitive plan from post-restore target readback, where it has the target
    file IDs required for a provider rename.
    """
    remote = report.get("remote_validation") or {}
    share = remote.get("share") or {}
    records = list(share.get("video_files") or [])
    selected_file_ids = list(remote.get("selected_file_ids") or [])
    selection_result = remote.get("selection_result") or {}
    episode_keys_by_file_id = {
        str(file_id).strip(): str(episode_key).strip()
        for episode_key, file_id in (selection_result.get("episode_file_map") or {}).items()
        if str(file_id).strip() and str(episode_key).strip()
    }
    if not records:
        return {"status": "NOT_CHECKED", "reason": "SHARE_SELECTION_RECORDS_UNAVAILABLE"}
    plan = build_rename_plan(
        records,
        selected_file_ids=selected_file_ids,
        destination_kind=str(payload.get("destination_kind") or "ongoing"),
        lifecycle_verified=bool(payload.get("lifecycle_verified")),
        title=str(payload.get("title") or getattr(resource, "title", "") or ""),
        year=payload.get("year") if payload.get("year") is not None else getattr(resource, "year", None),
        tmdb_id=getattr(resource, "tmdb_id", None),
        season=getattr(resource, "season", None),
        episode_key=(payload.get("episode_keys") or [getattr(resource, "episode_key", "")])[0],
        version_key=payload.get("version_key") or getattr(resource, "version_key", None),
        media_type=str(payload.get("media_type") or getattr(resource, "media_type", "tv") or "tv"),
        series_status=payload.get("series_status") or payload.get("tmdb_series_status"),
        content_complete=payload.get("content_complete"),
        total_episodes=payload.get("total_episodes"),
        collected_episodes=payload.get("collected_episodes"),
        inventory_count=payload.get("inventory_count"),
        cloud_count=payload.get("cloud_count"),
        active_transfer_count=payload.get("active_transfer_count"),
        aliases=list(payload.get("aliases") or []),
        episode_keys_by_file_id=episode_keys_by_file_id,
    )
    return plan.as_dict()


def classify_final_preflight(
    report: Mapping[str, Any],
    *,
    route: Mapping[str, Any] | None,
    route_error: str | None = None,
    rename_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return exactly one of ``AUTO_SAFE``, ``NEEDS_REVIEW``, ``REJECTED``."""
    if route_error:
        return {
            "classification": NEEDS_REVIEW,
            "reason": "DESTINATION_METADATA_UNAVAILABLE",
            "detail": str(route_error)[:500],
        }
    if not route or not route.get("media_root") or not route.get("media_category") or not route.get("inventory_prefix"):
        return {
            "classification": NEEDS_REVIEW,
            "reason": "CANONICAL_DESTINATION_INCOMPLETE",
            "detail": "media_root/media_category/inventory_prefix are required",
        }
    checks = report.get("checks") or {}
    if isinstance(checks, Mapping):
        check_map = checks
    else:
        check_map = {
            str(item.get("check")): {
                "result": "PASS" if item.get("ok") else "FAIL",
                "detail": item.get("detail"),
                "failure_code": item.get("failure_code"),
            }
            for item in checks
            if isinstance(item, Mapping) and item.get("check")
        }
    missing = sorted(_REQUIRED_CHECKS - set(check_map.keys()))
    if missing:
        return {
            "classification": NEEDS_REVIEW,
            "reason": "PREFLIGHT_CHECK_MISSING",
            "detail": ",".join(missing),
        }
    failures = []
    not_checked = []
    for name in sorted(_REQUIRED_CHECKS):
        item = check_map.get(name) or {}
        result = str(item.get("result") or "")
        if result == "FAIL":
            failures.append({
                "check": name,
                "failure_code": item.get("failure_code"),
                "detail": item.get("detail"),
            })
        elif result != "PASS":
            not_checked.append(name)
    if failures:
        transient = any(str(item.get("failure_code") or "") in _TRANSIENT for item in failures)
        return {
            "classification": NEEDS_REVIEW if transient else REJECTED,
            "reason": "TRANSIENT_PREFLIGHT_FAILURE" if transient else "DETERMINISTIC_PREFLIGHT_REJECTION",
            "detail": failures,
        }
    if not_checked:
        return {
            "classification": NEEDS_REVIEW,
            "reason": "PREFLIGHT_NOT_COMPLETE",
            "detail": not_checked,
        }
    if str(report.get("preflight_status") or "") != "CANARY_SAFE":
        return {
            "classification": NEEDS_REVIEW,
            "reason": "LEGACY_PREFLIGHT_STATUS_NOT_SAFE",
            "detail": report.get("preflight_status"),
        }
    if rename_plan is None:
        return {
            "classification": NEEDS_REVIEW,
            "reason": "RENAME_PLAN_MISSING",
            "detail": "rename planning must complete before ordinary transfer can restore",
        }
    rename_status = str(rename_plan.get("status") or "").strip()
    if rename_status == "RENAME_CONFLICT":
        return {
            "classification": REJECTED,
            "reason": "RENAME_CONFLICT",
            "detail": rename_plan.get("conflicts") or rename_plan.get("reason"),
        }
    if rename_status not in {
        "RENAME_READY",
        "RENAME_SKIPPED_KEEP_EXISTING_NAME",
        "RENAME_SKIPPED_STANDARD_CHINESE",
    }:
        return {
            "classification": NEEDS_REVIEW,
            "reason": "RENAME_PLAN_INCOMPLETE",
            "detail": rename_plan.get("reason") or rename_status or "rename status missing",
        }
    return {
        "classification": AUTO_SAFE,
        "reason": "ALL_FINAL_PREFLIGHT_CHECKS_PASS",
        "detail": "selection/share/episode/destination/duplicate/collected/inventory/rename inputs verified",
    }


__all__ = [
    "AUTO_SAFE",
    "NEEDS_REVIEW",
    "REJECTED",
    "build_source_rename_plan",
    "classify_final_preflight",
]
