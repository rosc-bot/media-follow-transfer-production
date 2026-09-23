"""Read-only end-to-end preflight for a single persisted batch task.

This diagnostic calls the same batch planner, remote listing validation and
rename planner as the ordinary Transfer Worker. It never claims or updates a
queue task and never invokes cloud write APIs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.database import AsyncSessionLocal
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.transfer.canary_preflight import preflight_task, validate_remote_canary
from app.transfer.final_preflight import (
    AUTO_SAFE,
    build_source_rename_plan,
    classify_final_preflight,
)
from app.transfer.queue_worker import (
    BATCH_PREFLIGHT_TIMEOUT_SECONDS,
    TransferQueueWorker,
    _safe_preflight_detail,
)
from app.transfer.status import REVIEW_STATUS, TransferStatus


async def inspect_batch_preflight(
    task_id: int,
    *,
    session_factory: async_sessionmaker = AsyncSessionLocal,
    timeout_seconds: float = BATCH_PREFLIGHT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Return a bounded, redacted final-preflight decision without DB writes."""
    worker = TransferQueueWorker(session_factory, worker_id="readonly-batch-preflight")
    report: dict[str, Any] = {
        "task_id": int(task_id),
        "read_only": True,
        "classification": "NEEDS_REVIEW",
        "reason": "BATCH_PREFLIGHT_NOT_RUN",
        "preflight_stage": "TASK_LOOKUP",
        "share_episode_count": 0,
        "missing_episode_keys": [],
        "episode_file_map_count": 0,
        "selected_file_ids_count": 0,
        "metadata_reconcile": {},
    }
    try:
        async with session_factory() as db, db.begin():
            task = await db.get(TransferQueueTask, int(task_id))
            if task is None:
                report.update(classification="REJECTED", reason="TASK_NOT_FOUND")
                return report
            report["task_status"] = str(task.status)
            if str(task.status) not in {
                str(TransferStatus.QUEUED),
                REVIEW_STATUS,
                str(TransferStatus.RETRY_WAIT),
                str(TransferStatus.RUNNING),
            }:
                report.update(classification="REJECTED", reason="TASK_STATUS_NOT_PREFLIGHTABLE")
                return report
            resource_id = int(task.resource_id) if task.resource_id is not None else None
            resource = await db.get(Resource, resource_id) if resource_id is not None else None
            if resource is None:
                report.update(classification="REJECTED", reason="RESOURCE_MISSING")
                return report
            payload = await worker._hydrate_payload(db, task)
            if str(payload.get("selection_mode") or "").upper() != "MISSING_EPISODES":
                from app.follow.episode_keys import canonical_episode_key

                keys = {
                    key
                    for value in (payload.get("episode_keys") or ([resource.episode_key] if resource.episode_key else []))
                    if (key := canonical_episode_key(int(resource.season or 1), value)) is not None
                }
                if len(keys) > 1:
                    payload["selection_mode"] = "MISSING_EPISODES"
            if str(payload.get("selection_mode") or "").upper() != "MISSING_EPISODES":
                report.update(classification="NEEDS_REVIEW", reason="NOT_A_BATCH_TASK")
                return report

            try:
                async with asyncio.timeout(float(timeout_seconds)):
                    report["preflight_stage"] = "SHARE_PROBE"
                    batch = await worker._batch_episode_presence_preflight(
                        db,
                        task=task,
                        resource=resource,
                        payload=payload,
                    )
                    share_evidence = batch.pop("_share_evidence", None)
                    batch.pop("_cloud_evidence", None)
                    cloud_verified = bool(batch.pop("_cloud_scan_verified", False))
                    cloud_keys = list(batch.pop("_cloud_verified_episode_keys", []) or [])
                    batch.pop("_watchlist_id", None)
                    batch.pop("_cloud_series_root_id", None)
                    batch.pop("_inventory_prefix", None)
                    batch.pop("_resource_title", None)
                    report["batch_presence"] = {
                        "classification": batch.get("classification"),
                        "reason": batch.get("reason"),
                        "season": batch.get("season"),
                        "share_episode_keys": list(batch.get("share_episode_keys") or []),
                        "missing_episode_keys": list(batch.get("missing_episode_keys") or []),
                        "presence_decisions": batch.get("presence_decisions") or {},
                        "metadata_reconcile": batch.get("metadata_reconcile") or {},
                        "episode_file_map_count": len(batch.get("episode_file_map") or {}),
                    }
                    report["cloud_scan_verified"] = cloud_verified
                    report["cloud_episode_count"] = len(cloud_keys)
                    report["share_episode_count"] = len(batch.get("share_episode_keys") or [])
                    report["missing_episode_keys"] = list(batch.get("missing_episode_keys") or [])
                    report["metadata_reconcile"] = batch.get("metadata_reconcile") or {}
                    classification = str(batch.get("classification") or "NEEDS_REVIEW")
                    reason = str(batch.get("reason") or "BATCH_PREFLIGHT_INCOMPLETE")
                    report["preflight_stage"] = str(payload.get("preflight_stage") or "FINAL_DECISION")
                    if classification != AUTO_SAFE:
                        report.update(classification=classification, reason=reason)
                        return report

                    payload["episode_keys"] = list(batch.get("missing_episode_keys") or [])
                    payload["selected_episode_keys"] = list(payload["episode_keys"])
                    payload["selection_mode"] = "MISSING_EPISODES"
                    async def remote_validator(**kwargs):
                        target = str(payload.get("target_folder_id") or kwargs.get("target_folder_id") or "")
                        validator_kwargs = {**kwargs, "target_folder_id": target}
                        if share_evidence is not None:
                            validator_kwargs["share_override"] = share_evidence
                        return await validate_remote_canary(**validator_kwargs)

                    preflight = await preflight_task(
                        db,
                        int(task_id),
                        remote_validator=remote_validator,
                        episode_keys_override=list(payload.get("episode_keys") or []),
                        selection_mode_override="MISSING_EPISODES",
                    )
                    remote = preflight.get("remote_validation") or {}
                    report["selection_mode"] = remote.get("selection_mode")
                    report["selected_file_ids_count"] = len(remote.get("selected_file_ids") or [])
                    report["selected_file_names_count"] = len(remote.get("selected_file_names") or [])
                    report["episode_file_map_count"] = len(remote.get("episode_file_map") or {})
                    rename_plan = build_source_rename_plan(preflight, payload=payload, resource=resource)
                    decision = classify_final_preflight(
                        preflight,
                        route={
                            "media_root": payload.get("media_root"),
                            "media_category": payload.get("media_category"),
                            "inventory_prefix": payload.get("inventory_prefix"),
                        },
                        rename_plan=rename_plan,
                    )
                    report["classification"] = str(decision.get("classification") or "NEEDS_REVIEW")
                    report["reason"] = str(decision.get("reason") or "FINAL_PREFLIGHT_INCOMPLETE")
                    report["preflight_stage"] = "FINAL_DECISION"
                    report["rename_status"] = rename_plan.get("status")
                    report["selected_episode_keys"] = list(remote.get("selected_episode_keys") or [])
            except TimeoutError as exc:
                report.update(
                    classification="NEEDS_REVIEW",
                    reason="BATCH_PREFLIGHT_TIMEOUT",
                    preflight_stage=str(payload.get("preflight_stage") or "UNKNOWN"),
                    detail=_safe_preflight_detail(exc),
                )
            except Exception as exc:  # noqa: BLE001 - diagnostic fails closed
                report.update(
                    classification="NEEDS_REVIEW",
                    reason="BATCH_PREFLIGHT_EXCEPTION",
                    preflight_stage=str(payload.get("preflight_stage") or report.get("preflight_stage") or "UNKNOWN"),
                    detail=_safe_preflight_detail(exc),
                )
    except Exception as exc:  # noqa: BLE001 - database/identity diagnostics fail closed
        report.update(
            classification="NEEDS_REVIEW",
            reason="BATCH_PREFLIGHT_EXCEPTION",
            preflight_stage=str(report.get("preflight_stage") or "TASK_LOOKUP"),
            detail=_safe_preflight_detail(exc),
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", type=int, action="append", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=BATCH_PREFLIGHT_TIMEOUT_SECONDS)
    args = parser.parse_args()

    async def run_all() -> list[dict[str, Any]]:
        results = []
        for task_id in args.task_id:
            results.append(await inspect_batch_preflight(task_id, timeout_seconds=args.timeout_seconds))
        return results

    results = asyncio.run(run_all())
    print(json.dumps(results, ensure_ascii=False, indent=2, sort_keys=True))
    if any(result.get("classification") != AUTO_SAFE for result in results):
        sys.exit(2)


if __name__ == "__main__":
    main()
