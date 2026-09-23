"""Phase 2E Canary preflight: all diagnostic checks are explicit and read-only.

This module deliberately does not execute a transfer. The execute command remains
separately gated in ``tools.run_transfer_canary``.
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.follow.bot_settings_service import BotSettingsService
from app.models.cloud import CloudConfig, CloudDiskInventory
from app.models.resource import Resource
from app.models.resource_candidate import ResourceCandidate
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.transfer.adapters import effective_cloud_write_enabled
from app.transfer.status import TransferStatus

CANARY_SAFE = "CANARY_SAFE"
CANARY_REJECTED = "CANARY_REJECTED"
NEEDS_REVIEW = "NEEDS_REVIEW"
PASS = "PASS"
FAIL = "FAIL"
NOT_CHECKED = "NOT_CHECKED"
EXECUTABLE_STATUSES = frozenset({TransferStatus.QUEUED, TransferStatus.RETRY_WAIT})
ALLOWED_SOURCE_TYPES = frozenset({"watchlist_scout", "framehdr"})
PERMANENT_CANDIDATE_STATUSES = frozenset({"INVALID_SHARE", "NO_VIDEO", "EPISODE_MISMATCH"})


class CheckReport(dict):
    """Named diagnostic checks with backward-compatible iteration for Phase 2D callers."""

    def __iter__(self):
        legacy_names = {
            "check_share_access": "share_readable",
            "check_video_files": "share_has_video",
            "check_episode_match": "episode_match",
            "check_account_auth": "guangya_auth_usable",
        }
        return iter([
            {"check": legacy_names.get(name, name), "ok": value["result"] == PASS, "detail": value["detail"]}
            for name, value in self.items()
        ])


def _check(result: str = NOT_CHECKED, detail: str = "", failure_code: str | None = None) -> dict:
    return {"result": result, "detail": detail, "failure_code": failure_code}


def _episode_numbers(keys: list[str]) -> set[int]:
    result: set[int] = set()
    for key in keys:
        try:
            result.add(int(str(key).rsplit("E", 1)[1]))
        except (IndexError, ValueError):
            continue
    return result


async def validate_remote_canary(
    *, provider: str, share_url: str, auth_ref: str, target_folder_id: str, episode_keys: list[str], season: int | None = None
) -> dict:
    """Run public-share and account-directory reads separately; no writes."""
    if provider != "guangya":
        return {
            "share_accessible": False,
            "has_video": False,
            "episode_match": False,
            "destination_auth": False,
            "destination_read": False,
            "failure_code": "UNSUPPORTED_PROVIDER",
            "failure_detail": f"provider={provider}",
        }
    from app.transfer.adapters.guangya import GuangyaAdapter
    from app.transfer.episode_matcher import diagnose_episode_match
    from app.transfer.share_probe import GuangyaShareProbe

    adapter = GuangyaAdapter(write_enabled=False)
    probe = GuangyaShareProbe(adapter=adapter)
    share = await probe.probe(share_url)
    result: dict[str, Any] = {
        "share_accessible": bool(share["share_accessible"]),
        "has_video": False,
        "episode_match": False,
        "destination_auth": False,
        "destination_read": False,
        "share": share,
        "episode_diagnostics": [],
        "failure_code": None,
        "failure_detail": "",
    }
    if not share["share_accessible"]:
        result["failure_code"] = str(share["error_code"])
        result["failure_detail"] = "; ".join(share["errors"])
    else:
        result["has_video"] = bool(share["video_names"])
        diagnostics = [
            row
            for key in episode_keys
            for row in diagnose_episode_match(
                target_episode_key=str(key), known_season=season, video_names=share["video_names"]
            )
        ]
        result["episode_diagnostics"] = diagnostics
        result["episode_match"] = bool(episode_keys) and all(
            any(row["match_result"] == "MATCH" for row in diagnostics_for_key)
            for diagnostics_for_key in (
                diagnose_episode_match(target_episode_key=str(key), known_season=season, video_names=share["video_names"])
                for key in episode_keys
            )
        )
        if not result["has_video"]:
            result["failure_code"] = "NO_VIDEO"
            result["failure_detail"] = "share read succeeded but contains no supported video file"
        elif not result["episode_match"]:
            result["failure_code"] = "EPISODE_MISMATCH"
            result["failure_detail"] = "no regular video filename strictly matches target episode"
    try:
        await adapter.list_directories(auth_token=auth_ref, parent_id=target_folder_id)
        result["destination_auth"] = True
        result["destination_read"] = True
    except Exception as exc:  # noqa: BLE001 - remote account verification is diagnostic/fail-closed
        result["failure_code"] = result["failure_code"] or "ACCOUNT_AUTH_FAILED"
        result["failure_detail"] = result["failure_detail"] or f"{type(exc).__name__}: {str(exc)[:240]}"
    return result


async def preflight_task(
    db: AsyncSession,
    task_id: int,
    *,
    remote_validator: Callable[..., Awaitable[dict]] | None = None,
) -> dict:
    """Produce a fully expanded read-only report for one historical task."""
    checks = CheckReport({
        name: _check()
        for name in (
            "check_task_status", "check_resource", "check_watchlist", "check_collected",
            "check_cloud_inventory", "check_duplicate_success", "check_duplicate_active",
            "check_share_url", "check_share_access", "check_video_files", "check_episode_match",
            "check_destination", "check_account_auth", "check_candidate_status", "check_source_type",
        )
    })
    report: dict[str, Any] = {
        "task_id": task_id,
        "title": None,
        "tmdb_id": None,
        "season": None,
        "episode_key": None,
        "checks": checks,
        "preflight_status": CANARY_SAFE,
        "execution_gate": "CLOSED",
        "failure_code": None,
        "failure_detail": "",
        "remote_validation": None,
    }

    def fail(name: str, code: str, detail: str, *, review: bool = False) -> None:
        checks[name] = _check(FAIL, detail, code)
        if report["failure_code"] is None:
            report["failure_code"] = code
            report["failure_detail"] = detail
        if review and report["preflight_status"] == CANARY_SAFE:
            report["preflight_status"] = NEEDS_REVIEW
        elif not review:
            report["preflight_status"] = CANARY_REJECTED

    def passed(name: str, detail: str) -> None:
        checks[name] = _check(PASS, detail)

    task = await db.get(TransferQueueTask, task_id)
    if task is None:
        fail("check_task_status", "TASK_NOT_FOUND", f"task #{task_id} does not exist")
        return report
    payload = dict(task.payload or {})
    resource = await db.get(Resource, task.resource_id) if task.resource_id else None
    report["title"] = payload.get("title") or (resource.title if resource else None)
    report["tmdb_id"] = payload.get("tmdb_id") or (resource.tmdb_id if resource else None)
    report["season"] = payload.get("season") or (resource.season if resource else None)
    episode_keys = list(payload.get("episode_keys") or ([resource.episode_key] if resource and resource.episode_key else []))
    report["episode_key"] = episode_keys[0] if len(episode_keys) == 1 else episode_keys
    tmdb_id, season = report["tmdb_id"], report["season"]
    share_url = payload.get("share_url") or (resource.share_url if resource else None)

    if task.status in EXECUTABLE_STATUSES:
        passed("check_task_status", f"status={task.status}")
    else:
        fail("check_task_status", "TASK_NOT_EXECUTABLE", f"status={task.status}", review=True)
    if resource is None:
        fail("check_resource", "RESOURCE_MISSING", f"resource_id={task.resource_id}")
    else:
        passed("check_resource", f"resource_id={resource.id}")

    watchlist = None
    if resource is None or not tmdb_id or not season:
        fail("check_watchlist", "IDENTITY_MISSING", "resource/tmdb_id/season missing")
        checks["check_collected"] = _check(NOT_CHECKED, "watchlist identity unavailable")
    else:
        rows = (await db.execute(select(SeriesWatchlist).where(
            SeriesWatchlist.tmdb_id == int(tmdb_id), SeriesWatchlist.season == int(season),
            SeriesWatchlist.status == "FOLLOWING",
        ))).scalars().all()
        matched = [row for row in rows if row.title == resource.title]
        if len(matched) == 1:
            watchlist = matched[0]
            passed("check_watchlist", f"FOLLOWING watchlist_id={watchlist.id}")
            collected = {str(key) for key in (watchlist.collected_episodes or [])}
            overlap = [key for key in episode_keys if str(key) in collected]
            if overlap:
                fail("check_collected", "ALREADY_COLLECTED", f"collected={overlap}")
            else:
                passed("check_collected", "not collected")
        else:
            fail("check_watchlist", "WATCHLIST_NOT_FOLLOWING", f"exact FOLLOWING match count={len(matched)}")
            checks["check_collected"] = _check(NOT_CHECKED, "watchlist identity unavailable")

    if tmdb_id and season:
        target_numbers = _episode_numbers(episode_keys)
        inventory = (await db.execute(select(CloudDiskInventory).where(
            CloudDiskInventory.tmdb_id == int(tmdb_id), CloudDiskInventory.season == int(season),
        ))).scalars().all()
        occupied = target_numbers & {int(row.episode) for row in inventory}
        if occupied:
            fail("check_cloud_inventory", "ALREADY_IN_CLOUD", f"episodes={sorted(occupied)}")
        else:
            passed("check_cloud_inventory", "no matching inventory")
    else:
        fail("check_cloud_inventory", "IDENTITY_MISSING", "tmdb_id/season unavailable")

    if tmdb_id:
        related = (await db.execute(select(TransferQueueTask))).scalars().all()
        target_keys = {str(key) for key in episode_keys}
        same = [
            row for row in related
            if row.id != task.id and int((row.payload or {}).get("tmdb_id") or -1) == int(tmdb_id)
            and target_keys & {str(key) for key in ((row.payload or {}).get("episode_keys") or [])}
        ]
        completed = [row.id for row in same if row.status == TransferStatus.COMPLETED]
        active = [row.id for row in same if row.status in {TransferStatus.QUEUED, TransferStatus.RETRY_WAIT, TransferStatus.RUNNING}]
        if completed:
            fail("check_duplicate_success", "DUPLICATE_SUCCESS", f"task_ids={completed}")
        else:
            passed("check_duplicate_success", "none")
        if active:
            fail("check_duplicate_active", "DUPLICATE_ACTIVE", f"task_ids={active}", review=True)
        else:
            passed("check_duplicate_active", "none")
    else:
        checks["check_duplicate_success"] = _check(NOT_CHECKED, "tmdb_id unavailable")
        checks["check_duplicate_active"] = _check(NOT_CHECKED, "tmdb_id unavailable")

    if share_url:
        passed("check_share_url", "present")
    else:
        fail("check_share_url", "SHARE_URL_MISSING", "no share URL")

    provider = str(payload.get("provider") or (resource.cloud_name if resource else "") or "guangya").lower()
    source_type = str(payload.get("source_type") or (resource.source_type if resource else "")).lower()
    if source_type in ALLOWED_SOURCE_TYPES:
        passed("check_source_type", source_type)
    else:
        fail("check_source_type", "SOURCE_TYPE_UNSUPPORTED", source_type or "missing")

    if resource is not None and tmdb_id and season and share_url:
        candidates = (await db.execute(select(ResourceCandidate).where(
            ResourceCandidate.tmdb_id == int(tmdb_id), ResourceCandidate.season == int(season),
            ResourceCandidate.episode_key.in_([str(key) for key in episode_keys]),
        ))).scalars().all()
        linked = [row for row in candidates if row.resource_id == resource.id or row.share_url == share_url]
        permanent = [row for row in linked if row.status in PERMANENT_CANDIDATE_STATUSES]
        if permanent:
            fail("check_candidate_status", "CANDIDATE_PERMANENTLY_INVALID", ",".join(f"{row.id}:{row.status}" for row in permanent))
        else:
            passed("check_candidate_status", ",".join(str(row.status) for row in linked) or "no linked ledger row")
    else:
        checks["check_candidate_status"] = _check(NOT_CHECKED, "identity/share unavailable")

    cloud_cfg = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
    if cloud_cfg is None or not cloud_cfg.enabled:
        fail("check_destination", "DESTINATION_INVALID", f"provider={provider} disabled/missing")
        checks["check_account_auth"] = _check(NOT_CHECKED, "cloud config unavailable")
    elif not (cloud_cfg.target_folder_id or cloud_cfg.ongoing_target_folder_id):
        fail("check_destination", "DESTINATION_INVALID", "no destination root")
        checks["check_account_auth"] = _check(NOT_CHECKED, "destination root unavailable")
    elif not cloud_cfg.auth_ref:
        passed("check_destination", "static destination root present")
        fail("check_account_auth", "ACCOUNT_AUTH_MISSING", "auth_ref absent")
    else:
        passed("check_destination", "static destination root present")
        if remote_validator is not None and share_url:
            remote = await remote_validator(
                provider=provider, share_url=str(share_url), auth_ref=str(cloud_cfg.auth_ref),
                target_folder_id=str(cloud_cfg.target_folder_id or cloud_cfg.ongoing_target_folder_id),
                episode_keys=[str(key) for key in episode_keys], season=int(season) if season else None,
            )
            remote = {
                **remote,
                "share_accessible": bool(remote.get("share_accessible", remote.get("share_readable", False))),
                "destination_auth": bool(remote.get("destination_auth", remote.get("auth_usable", False))),
                "destination_read": bool(remote.get("destination_read", remote.get("auth_usable", False))),
            }
            report["remote_validation"] = remote
            if remote.get("share_accessible"):
                passed("check_share_access", "read-only share API succeeded")
            else:
                fail("check_share_access", str(remote.get("failure_code") or "SHARE_UNREADABLE"), str(remote.get("failure_detail") or "share unreadable"))
            if remote.get("has_video"):
                passed("check_video_files", f"count={remote.get('share', {}).get('video_count', remote.get('video_count', '?'))}")
            elif remote.get("share_accessible"):
                fail("check_video_files", str(remote.get("failure_code") or "NO_VIDEO"), str(remote.get("failure_detail") or "no video"))
            else:
                checks["check_video_files"] = _check(NOT_CHECKED, "share unreadable")
            if remote.get("episode_match"):
                passed("check_episode_match", "strict episode filename match")
            elif remote.get("has_video"):
                fail("check_episode_match", str(remote.get("failure_code") or "EPISODE_MISMATCH"), str(remote.get("failure_detail") or "episode mismatch"))
            else:
                checks["check_episode_match"] = _check(NOT_CHECKED, "no readable video")
            if remote.get("destination_auth"):
                passed("check_account_auth", "account credentials accepted")
            else:
                fail("check_account_auth", str(remote.get("failure_code") or "ACCOUNT_AUTH_FAILED"), str(remote.get("failure_detail") or "destination auth failed"))
            if remote.get("destination_read"):
                passed("check_destination", "destination read succeeded")
            elif checks["check_destination"]["result"] == PASS:
                fail("check_destination", str(remote.get("failure_code") or "DESTINATION_READ_FAILED"), str(remote.get("failure_detail") or "destination read failed"))
        else:
            checks["check_share_access"] = _check(NOT_CHECKED, "remote validator not configured")
            checks["check_video_files"] = _check(NOT_CHECKED, "remote validator not configured")
            checks["check_episode_match"] = _check(NOT_CHECKED, "remote validator not configured")
            passed("check_account_auth", "auth_ref present; remote read not requested")

    paused = await BotSettingsService.is_transfer_paused(db)
    writes_enabled = effective_cloud_write_enabled(permit_canary_env=False)
    report["execution_gate"] = "OPEN" if not paused and writes_enabled else "CLOSED"
    report["transfer_paused"] = paused
    report["cloud_write_enabled"] = writes_enabled
    report["canary_cloud_write_env"] = os.getenv("CANARY_CLOUD_WRITE_ENABLED", "")
    report["verdict"] = report["preflight_status"]  # legacy compatibility
    report["rejections"] = [
        {
            "check": {
                "check_collected": "not_collected",
                "check_cloud_inventory": "not_in_cloud_inventory",
                "check_duplicate_success": "no_duplicate_success",
                "check_candidate_status": "candidate_not_invalid",
                "check_video_files": "share_has_video",
            }.get(name, name.removeprefix("check_")),
            "detail": value["detail"],
            "failure_code": value["failure_code"],
        }
        for name, value in checks.items()
        if value["result"] == FAIL
    ]
    report["reason"] = report["failure_code"] or "all resource checks passed"
    return report
