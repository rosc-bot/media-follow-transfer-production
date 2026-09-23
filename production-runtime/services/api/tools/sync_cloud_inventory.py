"""Read-only cloud reconciliation with an optional PostgreSQL inventory apply.

Usage (dry-run is the default)::

    python -m tools.sync_cloud_inventory --tmdb-id 322741 --season 1 --dry-run
    python -m tools.sync_cloud_inventory --tmdb-id 322741 --season 1 --apply

``--apply`` writes only the application ``cloud_disk_inventory`` table through
``CloudInventoryService``.  It never calls a provider write endpoint.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

import httpx
from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.models.cloud import CloudConfig
from app.models.resource import Resource
from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.cloud_inventory_service import CloudInventoryService, InventorySyncError
from app.transfer.guangya_auth import context_from_auth_ref


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile authenticated cloud files into CloudDiskInventory")
    parser.add_argument("--tmdb-id", type=int, required=True)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true", help="read cloud and DB only (default)")
    parser.add_argument("--apply", action="store_true", help="write only PostgreSQL CloudDiskInventory")
    return parser


def _error(code: str, detail: str) -> dict[str, Any]:
    return {"status": "FAILED", "error_code": code, "detail": detail}


async def _load_target(tmdb_id: int, season: int) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    async with AsyncSessionLocal() as db:
        resources = list((await db.scalars(
            select(Resource).where(
                Resource.tmdb_id == tmdb_id,
                Resource.season == season,
                Resource.transferred_folder_id.is_not(None),
            ).order_by(Resource.id.asc())
        )).all())
        if not resources:
            return None, _error("TARGET_NOT_FOUND", "no persisted transferred_folder_id for tmdb/season")
        # Prefer current verified completion evidence over legacy ACCEPTED rows.
        # A historical title may legitimately retain an older target folder while
        # a later completed task has its own season folder; merging both would
        # make a reconciliation scan ambiguous and unsafe.
        completed = [row for row in resources if str(row.status) == "COMPLETED"]
        selected = completed or [row for row in resources if str(row.status) == "ACCEPTED"]
        if not selected:
            selected = resources
        folder_ids = {str(row.transferred_folder_id).strip() for row in selected if row.transferred_folder_id}
        if len(folder_ids) != 1:
            return None, _error("TARGET_AMBIGUOUS", "multiple persisted target folders for selected transfer evidence")
        provider_names = {str(row.cloud_name or "guangya").strip().lower() for row in selected}
        if len(provider_names) != 1:
            return None, _error("PROVIDER_AMBIGUOUS", "multiple providers for selected transfer evidence")
        provider = next(iter(provider_names))
        config = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
        if config is None or not config.enabled:
            return None, _error("PROVIDER_UNAVAILABLE", "configured provider is missing or disabled")
        return {
            "title": next((row.title for row in selected if row.title), str(tmdb_id)),
            "folder_id": next(iter(folder_ids)),
            "provider": provider,
            "auth_ref": config.auth_ref,
            "selection_source": "COMPLETED_RESOURCE" if completed else "ACCEPTED_RESOURCE",
        }, None


async def _read_cloud(target: dict[str, Any]) -> tuple[list[dict], dict[str, Any] | None]:
    if target["provider"] != "guangya":
        return [], _error("UNSUPPORTED_PROVIDER", target["provider"])
    adapter = GuangyaAdapter(write_enabled=False)
    ctx = context_from_auth_ref(target.get("auth_ref"))
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            if not ctx.access_token:
                if not ctx.refresh_token:
                    return [], _error("AUTH_UNAVAILABLE", "no access or refresh credential available")
                access, refresh = await adapter.credential_provider.refresh_access(client, ctx.refresh_token)
                ctx.apply_refreshed(access, refresh)
            items = await adapter._list_folder_items(  # read-only provider listing
                client,
                parent_id=target["folder_id"],
                ctx=ctx,
            )
        return items, None
    except Exception as exc:  # noqa: BLE001 - classify without exposing provider details
        return [], _error("CLOUD_READ_FAILED", type(exc).__name__)


async def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.apply and args.dry_run:
        return _error("MUTUALLY_EXCLUSIVE_MODE", "choose --dry-run or --apply")
    if args.tmdb_id <= 0 or args.season <= 0:
        return _error("INVALID_IDENTITY", "tmdb-id and season must be positive")
    target, error = await _load_target(args.tmdb_id, args.season)
    if error:
        return error
    assert target is not None
    cloud_items, error = await _read_cloud(target)
    if error:
        return error
    async with AsyncSessionLocal() as db:
        plan = await CloudInventoryService.build_reconciliation_plan(
            db,
            tmdb_id=args.tmdb_id,
            season=args.season,
            title=target["title"],
            cloud_items=cloud_items,
            provider=target["provider"],
            rel_path_prefix=f"S{args.season:02d}",
        )
    result: dict[str, Any] = {
        "status": "DRY_RUN",
        "mode": "dry-run",
        "cloud_write_enabled": False,
        "inventory_write": False,
        "identity": {"tmdb_id": args.tmdb_id, "season": args.season, "title": target["title"]},
        "target_selection_source": target["selection_source"],
        "cloud_scan": {"status": "READ_ONLY_SCAN_OK", "items": len(cloud_items)},
        "plan": plan.as_dict(),
    }
    if not args.apply:
        return result

    async with AsyncSessionLocal() as db, db.begin():
        applied = await CloudInventoryService.apply_reconciliation_plan(db, plan)
    result.update({
        "status": "APPLIED",
        "mode": "apply",
        "inventory_write": True,
        "apply_scope": "POSTGRES_CLOUD_DISK_INVENTORY_ONLY",
        "applied": [item.as_dict() for item in applied],
        "applied_counts": {
            "inserted": sum(item.status == "INSERTED" for item in applied),
            "updated": sum(item.status == "UPDATED" for item in applied),
            "unchanged": sum(item.status == "UNCHANGED" for item in applied),
        },
    })
    return result


def main() -> int:
    args = _parser().parse_args()
    try:
        result = asyncio.run(run(args))
    except InventorySyncError as exc:
        result = _error("INVENTORY_PLAN_FAILED", str(exc))
    except Exception as exc:  # noqa: BLE001 - CLI emits a stable safe error
        result = _error("CLI_FAILED", type(exc).__name__)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") in {"DRY_RUN", "APPLIED"} else 1


if __name__ == "__main__":
    sys.exit(main())
