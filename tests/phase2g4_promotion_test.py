"""Phase 2G.4 promotion-only retry and inventory path contracts."""

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.bot_settings import BotSettings
from app.models.cloud import CloudDiskInventory
from app.models.resource import Resource
from app.models.transfer import TransferQueueTask
from app.transfer.errors import PromotionUnverifiedError
from app.transfer.notifier import build_promotion_card
from app.transfer.queue_worker import TransferQueueWorker
from app.transfer.status import TransferOutcome


class PromotionResumeOrchestrator:
    def __init__(self):
        self.calls = []
        self.restore_calls = 0

    async def execute(self, payload):
        self.calls.append(dict(payload))
        if len(self.calls) == 1:
            payload.update({
                "promotion_stage": "MOVED",
                "promotion_series_folder_id": "series-folder",
                "promotion_season_folder_id": "season-folder",
                "promotion_source_parent_id": "ongoing-parent",
                "promotion_destination_parent_id": "completed-parent",
            })
            raise PromotionUnverifiedError(
                "completed readback missing S01E02",
                series_folder_id="series-folder",
            )
        assert payload.get("promotion_stage") == "MOVED"
        return TransferOutcome(
            True,
            True,
            remote_folder_id="season-folder",
            remote_series_folder_id="series-folder",
            remote_files=("S01E01.mkv", "S01E02.mkv"),
            promotion_status="PROMOTION_COMPLETED",
            remote_destination_kind="completed",
        )


@pytest.mark.asyncio
async def test_promotion_failure_retries_only_readback_and_updates_inventory_paths(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/promotion-resume.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add(BotSettings(key="global_pause", val="0"))
        db.add(BotSettings(key="transfer_paused", val="0"))
        db.add(Resource(
            id=22,
            identity_key="promotion-resume",
            tmdb_id=22,
            title="测试归档剧",
            media_type="tv",
            season=1,
            episode=2,
            episode_key="S01E02",
            cloud_name="dry-run",
            share_url="https://pan.guangyapan.com/s/promotion",
            source_type="watchlist_scout",
            file_names=["S01E02.mkv"],
        ))
        db.add(TransferQueueTask(
            id=22,
            resource_id=22,
            idempotency_key="promotion-resume-task",
            payload={
                "provider": "dry-run",
                "operation": "promote",
                "promotion_source_series_folder_id": "ongoing-series",
                "series_folder_name": "测试归档剧 {tmdbid-22}",
                "episode_keys": ["promotion"],
            },
        ))
        db.add_all([
            CloudDiskInventory(title="测试归档剧", clean_title="测试归档剧", tmdb_id=22, season=1, episode=1, file_name="S01E01.mkv", rel_path="测试归档剧 {tmdbid-22}/S01/S01E01.mkv"),
            CloudDiskInventory(title="测试归档剧", clean_title="测试归档剧", tmdb_id=22, season=1, episode=2, file_name="S01E02.mkv", rel_path="测试归档剧 {tmdbid-22}/S01/S01E02.mkv"),
        ])
    orchestrator = PromotionResumeOrchestrator()
    worker = TransferQueueWorker(sessions, orchestrator=orchestrator)
    assert await worker.process_once() is True
    async with sessions() as db:
        task = await db.scalar(select(TransferQueueTask).where(TransferQueueTask.id == 22))
        assert task.status == "RETRY_WAIT"
        assert task.payload["promotion_stage"] == "MOVED"
    assert await worker.process_once() is True
    async with sessions() as db:
        task = await db.scalar(select(TransferQueueTask).where(TransferQueueTask.id == 22))
        rows = list((await db.scalars(select(CloudDiskInventory).where(CloudDiskInventory.tmdb_id == 22).order_by(CloudDiskInventory.episode))).all())
        assert task.status == "COMPLETED"
        assert [row.rel_path for row in rows] == [
            "测试归档剧 {tmdbid-22}/S01/S01E01.mkv",
            "测试归档剧 {tmdbid-22}/S01/S01E02.mkv",
        ]
    assert len(orchestrator.calls) == 2
    assert orchestrator.restore_calls == 0
    await engine.dispose()


class PreMoveFailureOrchestrator:
    async def execute(self, _payload):
        raise PromotionUnverifiedError("TMDB root identity is ambiguous")


@pytest.mark.asyncio
async def test_promotion_error_before_move_is_review_not_a_moved_retry(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/promotion-before-move.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add(BotSettings(key="global_pause", val="0"))
        db.add(BotSettings(key="transfer_paused", val="0"))
        db.add(Resource(
            id=23, identity_key="promotion-before-move", tmdb_id=23, title="待核验剧",
            media_type="tv", season=1, episode=1, episode_key="S01E01", cloud_name="dry-run",
            share_url="https://example/s/promotion", source_type="watchlist_scout",
        ))
        db.add(TransferQueueTask(
            id=23, resource_id=23, idempotency_key="promotion-before-move-task",
            payload={"provider": "dry-run", "operation": "promote", "promotion_source_series_folder_id": "source"},
        ))
    worker = TransferQueueWorker(sessions, orchestrator=PreMoveFailureOrchestrator())

    assert await worker.process_once() is False
    async with sessions() as db:
        task = await db.get(TransferQueueTask, 23)
        assert task.status == "PENDING"
        assert task.payload["promotion_status"] == "NEEDS_REVIEW"
        assert "promotion_stage" not in task.payload
    await engine.dispose()


def test_promotion_notification_is_preview_only_template():
    text = build_promotion_card(
        task_payload={"title": "测试归档剧", "series_folder_name": "测试归档剧 {tmdbid-22}"},
        promotion_result={"total_expected": 8},
    )
    assert "剧集已完结归档" in text
    assert "测试归档剧" in text
    assert "8" in text
    assert "最终目录" in text
