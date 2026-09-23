"""Read-only Phase 2E diagnostics for shares that failed episode matching."""

from __future__ import annotations

import argparse
import asyncio
import json

from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.episode_matcher import diagnose_episode_match
from app.transfer.share_probe import GuangyaShareProbe


async def diagnose(*, task_ids: list[int] | None = None, limit: int = 20) -> dict:
    adapter = GuangyaAdapter(write_enabled=False)
    probe = GuangyaShareProbe(adapter=adapter)
    rows = []
    async with AsyncSessionLocal() as db:
        statement = select(TransferQueueTask).where(TransferQueueTask.status.in_(["QUEUED", "RETRY_WAIT"])).order_by(TransferQueueTask.id)
        if task_ids:
            statement = statement.where(TransferQueueTask.id.in_(task_ids))
        tasks = (await db.execute(statement.limit(limit))).scalars().all()
        for task in tasks:
            resource = await db.get(Resource, task.resource_id)
            payload = task.payload or {}
            episode_key = str((payload.get("episode_keys") or [resource.episode_key if resource else ""])[0])
            season = payload.get("season") or (resource.season if resource else None)
            share_url = payload.get("share_url") or (resource.share_url if resource else None)
            if not share_url:
                continue
            share = await probe.probe(str(share_url))
            rows.append({
                "task_id": task.id,
                "title": payload.get("title") or (resource.title if resource else None),
                "target": episode_key,
                "share_error_code": share["error_code"],
                "video_names": share["video_names"],
                "files": diagnose_episode_match(target_episode_key=episode_key, known_season=int(season) if season else None, video_names=share["video_names"]) if share["share_accessible"] else [],
            })
    return {"rows": rows, "total": len(rows)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only episode matcher diagnosis")
    parser.add_argument("--task-id", type=int, action="append")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(diagnose(task_ids=args.task_id, limit=args.limit)), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
