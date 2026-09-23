"""Phase 2G.4 transfer-stage regression tests."""

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.bot_settings import BotSettings
from app.models.cloud import CloudDiskInventory
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.errors import RenameUnverifiedError
from app.transfer.guangya_auth import context_from_auth_ref
from app.transfer.notifier import NotificationResult
from app.transfer.queue_worker import TransferQueueWorker
from app.transfer.status import TransferOutcome


class RenameReadbackAdapter(GuangyaAdapter):
    def __init__(self):
        super().__init__(write_enabled=True)
        self.rename_calls = []
        self.items = [
            {"fileId": "target-file", "name": " #追新转存  S01E02-Gyy.mkv", "resType": 1, "size": 1234},
            {"fileId": "other-file", "name": "S01E01-Gyy.mkv", "resType": 1},
        ]

    async def _list_folder_items(self, client, *, parent_id, ctx):
        return list(self.items)

    async def _authorized_post(self, client, url, payload, ctx):
        self.rename_calls.append((url, dict(payload)))
        if url.endswith("/file/rename"):
            for item in self.items:
                if item["fileId"] == payload["fileId"]:
                    item["name"] = payload["newName"]
        return {"code": 0, "data": {}}


@pytest.mark.asyncio
async def test_rename_stage_calls_only_selected_file_and_readback_verifies_old_name_gone():
    adapter = RenameReadbackAdapter()
    payload = {"destination_kind": "ongoing", "title": "测试剧", "season": 1, "episode_keys": ["S01E02"]}
    records, status = await adapter._rename_selected_verified_files(
        object(),
        target_id="target-folder",
        ctx=context_from_auth_ref("access-token"),
        records=list(adapter.items),
        selected_names={"#追新转存  S01E02-Gyy.mkv"},
        payload=payload,
    )
    assert status == "RENAME_VERIFIED"
    assert len(adapter.rename_calls) == 1
    assert adapter.rename_calls[0][1] == {"fileId": "target-file", "newName": "测试剧.S01E02.mkv"}
    assert {row["name"] for row in records} == {"S01E01-Gyy.mkv", "测试剧.S01E02.mkv"}


class RenameResumeOrchestrator:
    def __init__(self):
        self.calls = []

    async def execute(self, payload):
        self.calls.append(dict(payload))
        if len(self.calls) == 1:
            payload["execution_stage"] = "RENAMING"
            payload["remote_folder_id"] = "verified-folder"
            raise RenameUnverifiedError(
                "rename endpoint failed after restore readback",
                remote_folder_id="verified-folder",
                remote_records=[{"fileId": "target-file", "name": "source.mkv", "resType": 1}],
            )
        assert payload["execution_stage"] == "RENAMING"
        payload["selected_file_names"] = ["S01E02.mkv"]
        payload["expected_files"] = ["S01E02.mkv"]
        return TransferOutcome(
            True,
            True,
            remote_folder_id="verified-folder",
            remote_files=("S01E02.mkv",),
            remote_file_records=({"file_id": "target-file", "name": "S01E02.mkv", "size": 10},),
            rename_status="RENAME_VERIFIED",
        )


class SilentNotifier:
    async def notify_success_result(self, **kwargs):
        return NotificationResult("SENT", True, target_source="test")


@pytest.mark.asyncio
async def test_rename_failure_retries_from_rename_stage_without_second_restore(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/rename-resume.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add(BotSettings(key="global_pause", val="0"))
        db.add(BotSettings(key="transfer_paused", val="0"))
        db.add(Resource(
            id=1,
            identity_key="rename-resume",
            tmdb_id=11,
            title="测试剧",
            media_type="tv",
            season=1,
            episode=2,
            episode_key="S01E02",
            cloud_name="dry-run",
            share_url="https://pan.guangyapan.com/s/x",
            source_type="watchlist_scout",
            file_names=["source.mkv"],
        ))
        db.add(TransferQueueTask(
            id=1,
            resource_id=1,
            idempotency_key="rename-resume-task",
            payload={"provider": "dry-run", "episode_keys": ["S01E02"], "expected_files": ["source.mkv"]},
        ))
    orchestrator = RenameResumeOrchestrator()
    worker = TransferQueueWorker(sessions, orchestrator=orchestrator, notifier=SilentNotifier())
    assert await worker.process_once() is True
    async with sessions() as db:
        first = await db.scalar(select(TransferQueueTask).where(TransferQueueTask.id == 1))
        assert first.status == "RETRY_WAIT"
        assert first.payload["execution_stage"] == "RENAMING"
        assert first.payload["remote_folder_id"] == "verified-folder"
    assert await worker.process_once() is True
    async with sessions() as db:
        final = await db.scalar(select(TransferQueueTask).where(TransferQueueTask.id == 1))
        assert final.status == "COMPLETED"
        assert final.payload["execution_stage"] == "RENAMING"
        resource = await db.scalar(select(Resource).where(Resource.id == 1))
        inventory = await db.scalar(select(CloudDiskInventory).where(CloudDiskInventory.tmdb_id == 11))
        assert resource.file_names == ["S01E02.mkv"]
        assert inventory.file_name == "S01E02.mkv"
    assert len(orchestrator.calls) == 2
    await engine.dispose()
