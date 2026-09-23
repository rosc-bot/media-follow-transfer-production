"""Read-only Phase 2G.5 audit: naming, notification and promotion previews.

This tool performs SELECTs and provider list APIs only.  It never claims a
preview is a rename/transfer/promotion, never enqueues a task and never sends a
Telegram message.  It is intended to run inside the deployed application image
against the configured PostgreSQL database.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Iterable
from typing import Any

from sqlalchemy import select

from app.core.config import get_settings
from app.core.database import AsyncSessionLocal
from app.follow.bot_settings_service import BotSettingsService
from app.follow.completion_promotion_service import CompletionPromotionService
from app.follow.promotion import _ENDED, _status
from app.models.channel import ChannelSetting
from app.models.cloud import CloudConfig, CloudDiskInventory
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.models.watchlist import SeriesWatchlist
from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.notifier import (
    build_success_card,
    resolve_poster_url,
    sanitize_share_url,
)
from app.transfer.rename import build_rename_plan, has_meaningful_media_name

DEFAULT_TASK_IDS = (1301, 1302, 1303, 1307)


def _json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list, tuple)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _names_and_sizes(task: TransferQueueTask, resource: Resource) -> tuple[list[str], dict[str, int], list[dict[str, Any]]]:
    payload = _json(task.payload, {})
    result = _json(task.result, {})
    names = list(result.get("selected_file_names") or payload.get("selected_file_names") or result.get("remote_files") or payload.get("expected_files") or resource.file_names or [])
    names = list(dict.fromkeys(str(name).strip() for name in names if str(name).strip()))
    sizes: dict[str, int] = {}
    for source in (result.get("selected_file_sizes"), payload.get("selected_file_sizes")):
        if isinstance(source, dict):
            for name, value in source.items():
                try:
                    sizes[str(name).strip()] = int(value or 0)
                except (TypeError, ValueError):
                    continue
    records = result.get("remote_file_records") or payload.get("remote_file_records") or []
    normalized_records: list[dict[str, Any]] = []
    for index, item in enumerate(records):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("fileName") or "").strip()
        if not name:
            continue
        record = {
            "file_id": str(item.get("file_id") or item.get("fileId") or item.get("id") or f"historical-{task.id}-{index}"),
            "name": name,
            "size": item.get("size") or item.get("fileSize") or 0,
        }
        normalized_records.append(record)
        try:
            if name not in sizes:
                sizes[name] = int(record["size"] or 0)
        except (TypeError, ValueError):
            pass
    for name in names:
        if not any(item["name"] == name for item in normalized_records):
            normalized_records.append({"file_id": f"historical-{task.id}-{len(normalized_records)}", "name": name, "size": sizes.get(name, 0)})
    return names, sizes, normalized_records


def _content_complete(*, watchlist: SeriesWatchlist | None, inventory_count: int, cloud_keys: set[str], scan_status: str, active_count: int) -> bool:
    if watchlist is None or _status(watchlist.tmdb_series_status) not in _ENDED:
        return False
    total = int(watchlist.total_episodes or watchlist.last_aired_episode or 0)
    if total <= 0 or scan_status != "VERIFIED" or active_count:
        return False
    season = int(watchlist.season or 1)
    collected = {
        str(value).strip()
        for value in watchlist.collected_episodes or []
        if str(value).strip()
    }
    expected = {f"S{season:02d}E{episode:02d}" for episode in range(1, total + 1)}
    return len(collected) >= total and inventory_count >= total and expected.issubset(cloud_keys)


async def _task_preview(db, task_id: int, *, prefiltered_tmdb_ids: set[int]) -> dict[str, Any]:
    task = await db.get(TransferQueueTask, task_id)
    if task is None:
        return {"task_id": task_id, "error": "TASK_NOT_FOUND"}
    resource = await db.get(Resource, task.resource_id)
    if resource is None:
        return {"task_id": task_id, "error": "RESOURCE_NOT_FOUND"}
    payload = _json(task.payload, {})
    watchlist = await db.scalar(select(SeriesWatchlist).where(
        SeriesWatchlist.tmdb_id == resource.tmdb_id,
        SeriesWatchlist.season == resource.season,
    ).order_by(SeriesWatchlist.id.asc())) if resource.tmdb_id and resource.season else None
    inventory_rows = list((await db.scalars(select(CloudDiskInventory).where(
        CloudDiskInventory.tmdb_id == resource.tmdb_id,
        CloudDiskInventory.season == resource.season,
    ).order_by(CloudDiskInventory.episode.asc(), CloudDiskInventory.id.asc()))).all()) if resource.tmdb_id and resource.season else []
    active_count = await CompletionPromotionService._active_transfer_count(
        db, tmdb_id=resource.tmdb_id, season=resource.season,
    ) if resource.tmdb_id and resource.season else 0
    names, sizes, records = _names_and_sizes(task, resource)
    current_name = names[0] if names else ""
    scan: dict[str, Any] = {
        "scan_status": "SCAN_NOT_RUN",
        "cloud_episode_keys_by_season": {},
        "file_count": 0,
        "scan_watermark": None,
    }
    conflict: dict[str, Any] = {"status": "NOT_RUN", "conflict": False, "recursive_scan": False}
    cloud_config = await db.scalar(select(CloudConfig).where(CloudConfig.name == (resource.cloud_name or "guangya")))
    adapter: GuangyaAdapter | None = None
    if watchlist is not None and cloud_config is not None and resource.tmdb_id and watchlist.remote_series_folder_id:
        if str(resource.cloud_name or "guangya").casefold() == "guangya" and cloud_config.auth_ref:
            adapter = GuangyaAdapter(write_enabled=False)
            scan = await adapter.scan_series_root_readonly(
                auth_token=str(cloud_config.auth_ref),
                tmdb_id=int(resource.tmdb_id),
                series_root_id=str(watchlist.remote_series_folder_id),
                relevant_seasons=[int(watchlist.season or 1)],
                timeout_seconds=30,
                max_depth=6,
                max_items=5000,
                page_size=100,
            )
            conflict = await adapter.inspect_completed_root_conflict_readonly(
                auth_token=str(cloud_config.auth_ref),
                completed_root_id=str(cloud_config.target_folder_id or ""),
                tmdb_id=int(resource.tmdb_id),
                title=str(watchlist.title or resource.title or ""),
            )
        else:
            scan["scan_status"] = "API_ERROR"
            scan["error"] = "UNSUPPORTED_OR_MISSING_READONLY_PROVIDER_AUTH"
            conflict = {"status": "UNVERIFIED", "conflict": True, "error": "PROVIDER_AUTH_UNAVAILABLE"}
    if adapter is not None and resource.share_url and current_name and not sizes.get(current_name):
        try:
            source_listing = await adapter.inspect_share(
                share_url=resource.share_url,
                max_depth=6,
                max_items=5000,
                max_pages=100,
            )
            matched_size = 0
            for source_item in source_listing.get("files") or []:
                source_name = str(source_item.get("name") or "").strip()
                if source_name == current_name:
                    try:
                        matched_size = int(source_item.get("size") or source_item.get("fileSize") or 0)
                    except (TypeError, ValueError):
                        matched_size = 0
                    if matched_size:
                        sizes[current_name] = matched_size
                        break
            scan["source_share_size_lookup"] = {
                "status": "VERIFIED" if matched_size else "NO_SIZE",
                "matched_file": current_name,
                "size": matched_size,
            }
        except Exception as exc:  # noqa: BLE001 - read-only audit fallback
            scan["source_share_size_lookup"] = {
                "status": "UNVERIFIED",
                "matched_file": current_name,
                "error": type(exc).__name__,
            }
    scan_keys = {
        int(season): {str(value) for value in values}
        for season, values in (scan.get("cloud_episode_keys_by_season") or {}).items()
    }
    for cloud_name, cloud_size in (scan.get("file_sizes_by_name") or {}).items():
        try:
            sizes.setdefault(str(cloud_name), int(cloud_size or 0))
        except (TypeError, ValueError):
            continue
    season_keys = scan_keys.get(int(resource.season or 1), set())
    complete = _content_complete(
        watchlist=watchlist,
        inventory_count=len(inventory_rows),
        cloud_keys=season_keys,
        scan_status=str(scan.get("scan_status") or ""),
        active_count=active_count,
    )
    plan = build_rename_plan(
        records,
        selected_file_ids=[records[0]["file_id"]] if records else [],
        title=str(resource.title or (watchlist.title if watchlist else "")),
        year=(watchlist.year if watchlist else resource.year),
        tmdb_id=resource.tmdb_id,
        season=resource.season,
        episode_key=payload.get("episode_key") or resource.episode_key,
        media_type=resource.media_type,
        series_status=(watchlist.tmdb_series_status if watchlist else payload.get("series_status")),
        content_complete=complete,
        total_episodes=(watchlist.total_episodes if watchlist else None),
        collected_episodes=(watchlist.collected_episodes if watchlist else []),
        inventory_count=len(inventory_rows),
        cloud_count=len(season_keys),
        active_transfer_count=active_count,
    )
    final_names = [operation.new_name for operation in plan.operations] or ([current_name] if current_name else [])
    if plan.decision == "KEEP":
        final_names = names
    enriched = {
        **payload,
        "title": resource.title or (watchlist.title if watchlist else "影视资源"),
        "tmdb_id": resource.tmdb_id,
        "media_type": resource.media_type,
        "year": watchlist.year if watchlist else resource.year,
        "season": resource.season,
        "episode_keys": payload.get("episode_keys") or ([resource.episode_key] if resource.episode_key else []),
        "series_status": watchlist.tmdb_series_status if watchlist else payload.get("series_status"),
        "tmdb_series_status": watchlist.tmdb_series_status if watchlist else payload.get("tmdb_series_status"),
        "total_episodes": watchlist.total_episodes if watchlist else payload.get("total_episodes"),
        "collected_episodes": watchlist.collected_episodes if watchlist else payload.get("collected_episodes"),
        "content_complete": complete,
        "inventory_count": len(inventory_rows),
        "cloud_count": len(season_keys),
        "active_transfer_count": active_count,
        "share_url": resource.share_url,
        "archive_directory": payload.get("remote_rel_path_prefix") or payload.get("archive_directory") or f"影视转存 ongoing/{resource.title or '影视资源'}/S{int(resource.season or 1):02d}",
        "selected_file_names": final_names,
        "source_type": resource.source_type,
    }
    poster = await resolve_poster_url(enriched)
    if poster.get("url"):
        enriched["poster_url"] = poster["url"]
    card = build_success_card(
        task_payload=enriched,
        transfer_result={
            "verified": True,
            "selected_file_names": final_names,
            "remote_files": final_names,
            "selected_file_sizes": {name: sizes.get(name, sizes.get(current_name, 0)) for name in final_names},
            "remote_file_records": records,
        },
    )
    promotion: dict[str, Any]
    if watchlist is None:
        promotion = {"decision": "NEEDS_REVIEW", "reason": "WATCHLIST_NOT_FOUND"}
    else:
        decision = await CompletionPromotionService.build_dry_run(
            db,
            watchlist=watchlist,
            cloud_episode_keys_by_season=scan_keys if scan.get("scan_status") == "VERIFIED" else None,
            destination_conflict=bool(conflict.get("conflict")),
            promotion_evaluation_id=f"audit:{task_id}",
            cloud_scan_status=scan.get("scan_status"),
            cloud_scan_timestamp=scan.get("scan_timestamp"),
            cloud_scan_watermark=scan.get("scan_watermark"),
            require_cloud_scan_watermark=True,
        )
        promotion = decision.as_dict()
    return {
        "task_id": task_id,
        "title": resource.title,
        "tmdb_id": resource.tmdb_id,
        "series_status": watchlist.tmdb_series_status if watchlist else None,
        "total_expected": watchlist.total_episodes if watchlist else None,
        "collected_count": len(watchlist.collected_episodes or []) if watchlist else 0,
        "inventory_count": len(inventory_rows),
        "cloud_count": len(season_keys),
        "active_transfer_count": active_count,
        "current_file_name": current_name,
        "has_meaningful_media_name": has_meaningful_media_name(
            current_name,
            media_type=resource.media_type,
            title=resource.title or (watchlist.title if watchlist else None),
        ),
        "rename_plan": plan.as_dict(),
        "rename_decision": plan.decision,
        "standard_chinese_name": final_names[0] if final_names and plan.decision == "RENAME_STANDARD_CHINESE" else None,
        "rename_required": plan.rename_required,
        "conflict": list(plan.conflicts),
        "poster_resolution": poster,
        "notification_preview": {
            "bot": get_settings().telegram_bot_username,
            "chat": get_settings().transfer_success_chat,
            "send_method": "sendPhoto" if card.poster_url else "sendMessage",
            "poster_url": card.poster_url,
            "caption_preview": card.caption,
            "share_url": sanitize_share_url(resource.share_url),
            "status": card.business_status,
            "episode": enriched["episode_keys"],
            "selected_file": final_names,
            "file_size": card.selected_size_bytes,
            "quality": next((line.split('</b>', 1)[-1] for line in card.caption.splitlines() if "规格版本" in line), "其他"),
            "archive_directory": enriched["archive_directory"],
            "contributor": enriched.get("contributor_username") or enriched.get("source_channel_title") or "自动打捞 / FrameHDR",
            "preview_only": True,
        },
        "physical_scan": scan,
        "completed_root_conflict": conflict,
        "promotion_prefilter_candidate": int(resource.tmdb_id or 0) in prefiltered_tmdb_ids,
        "promotion": promotion,
    }


async def audit(task_ids: Iterable[int]) -> dict[str, Any]:
    settings = get_settings()
    async with AsyncSessionLocal() as db:
        prefiltered = await CompletionPromotionService.prefilter_watchlists(db)
        prefiltered_tmdb_ids = {int(item.tmdb_id) for item in prefiltered}
        previews = []
        for task_id in task_ids:
            try:
                previews.append(await _task_preview(db, int(task_id), prefiltered_tmdb_ids=prefiltered_tmdb_ids))
            except Exception as exc:  # noqa: BLE001 - retain per-task audit evidence
                previews.append({"task_id": int(task_id), "error": f"{type(exc).__name__}: {str(exc)[:300]}"})
        settings_rows = {}
        for key in ("global_pause", "follow_paused", "transfer_paused"):
            settings_rows[key] = await BotSettingsService.get(db, key, "unknown")
        channels = [
            {
                "channel_id": row.channel_id,
                "channel_name": row.channel_name,
                "enabled": row.enabled,
                "role": row.role,
                "transfer_mode": row.transfer_mode,
            }
            for row in (await db.scalars(select(ChannelSetting).order_by(ChannelSetting.id.asc()))).all()
        ]
    return {
        "read_only": True,
        "task_ids": [int(value) for value in task_ids],
        "security_gates": {
            **settings_rows,
            "CLOUD_WRITE_ENABLED": bool(settings.cloud_write_enabled),
            "CANARY_CLOUD_WRITE_ENABLED": bool(settings.canary_cloud_write_enabled),
        },
        "prefilter_candidate_count": len(prefiltered),
        "publish_only_channels": [row for row in channels if row["role"] == "PUBLISH_ONLY"],
        "channels": channels,
        "previews": previews,
        "real_actions": {
            "transfer": 0,
            "restore": 0,
            "rename": 0,
            "promotion_move": 0,
            "resource_publish": 0,
            "telegram_send": 0,
            "telegram_session_modified": 0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 2G.5 read-only audit")
    parser.add_argument("--task-id", dest="task_ids", action="append", type=int)
    args = parser.parse_args()
    task_ids = tuple(args.task_ids or DEFAULT_TASK_IDS)
    print(json.dumps(asyncio.run(audit(task_ids)), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
