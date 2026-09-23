"""Phase 2G.3 Telegram routing, admin authorization and PUBLISH_ONLY tests."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.constants import CHANNEL_ROLE_PUBLISH_ONLY
from app.core.database import Base
from app.ingest.ingest_policy import decide_ingest
from app.models.admin import AdminAuditLog, TelegramAdmin
from app.models.channel import ChannelSetting
from app.monitor.event_router import EventRouter
from app.security.admin_service import AdminPermissionError, AdminService
from app.telegram.route_verifier import TelegramRouteVerifier
from app.transfer.notifier import TransferNotifier


@pytest.fixture
def session_factory(tmp_path):
    async def build():
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/admin.db")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        return engine, factory
    return build


@pytest.mark.asyncio
async def test_owner_add_admin_idempotent_username_change_and_audit(session_factory):
    engine, factory = await session_factory()
    try:
        async with factory() as db:
            db.add(TelegramAdmin(telegram_user_id=8586984520, role="OWNER", enabled=True))
            await db.commit()
            first = await AdminService.add_admin(
                db,
                actor_user_id=8586984520,
                target_user_id=123456789,
                username="old_name",
                display_name="Old",
            )
            await db.commit()
            second = await AdminService.add_admin(
                db,
                actor_user_id=8586984520,
                target_user_id=123456789,
                username="new_name",
                display_name="New",
            )
            await db.commit()
            rows = list((await db.scalars(select(TelegramAdmin).where(TelegramAdmin.role == "ADMIN"))).all())
            audits = list((await db.scalars(select(AdminAuditLog).where(AdminAuditLog.action == "ADD_ADMIN"))).all())
            assert first.telegram_user_id == second.telegram_user_id == 123456789
            assert len(rows) == 1
            assert rows[0].username == "new_name"
            assert await AdminService.is_admin(db, 123456789) is True
            assert len(audits) == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_disable_remove_admin_and_owner_protection(session_factory):
    engine, factory = await session_factory()
    try:
        async with factory() as db:
            db.add(TelegramAdmin(telegram_user_id=8586984520, role="OWNER", enabled=True))
            await db.commit()
            await AdminService.add_admin(db, actor_user_id=8586984520, target_user_id=111)
            await db.commit()
            await AdminService.set_enabled(db, actor_user_id=8586984520, target_user_id=111, enabled=False)
            await db.commit()
            assert await AdminService.is_admin(db, 111) is False
            with pytest.raises(AdminPermissionError, match="OWNER_CANNOT_BE_MODIFIED"):
                await AdminService.set_enabled(db, actor_user_id=8586984520, target_user_id=8586984520, enabled=False)
            await AdminService.set_enabled(db, actor_user_id=8586984520, target_user_id=111, enabled=True)
            await AdminService.remove_admin(db, actor_user_id=8586984520, target_user_id=111)
            await db.commit()
            assert await AdminService.is_admin(db, 111) is False
            actions = [row.action for row in (await db.scalars(select(AdminAuditLog))).all()]
            assert "DISABLE_ADMIN" in actions
            assert "ENABLE_ADMIN" in actions
            assert "REMOVE_ADMIN" in actions
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_admin_cannot_manage_admin_and_fallback_owner_is_fail_safe(session_factory):
    engine, factory = await session_factory()
    try:
        async with factory() as db:
            db.add(TelegramAdmin(telegram_user_id=8586984520, role="OWNER", enabled=True))
            db.add(TelegramAdmin(telegram_user_id=222, role="ADMIN", enabled=True))
            await db.commit()
            assert await AdminService.is_owner(db, 8586984520) is True
            with pytest.raises(AdminPermissionError, match="OWNER_REQUIRED"):
                await AdminService.add_admin(db, actor_user_id=222, target_user_id=333)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_publish_only_is_hard_rejected_by_ingest_policy_and_router():
    setting = ChannelSetting(channel_id="-1004332079561", role=CHANNEL_ROLE_PUBLISH_ONLY, enabled=True, transfer_mode="AUTO")
    decision = decide_ingest(source_type="telegram_channel", is_forward=False, setting=setting)
    assert decision.accepted is False
    assert decision.auto_transfer is False
    assert "PUBLISH_ONLY" in decision.reason

    class Chat:
        id = -1004332079561
        title = "发布频道"
        username = "guangyaziyuanfenxiang"
        broadcast = True
        first_name = None

    class Message:
        id = 1
        text = "新资源 https://pan.guangyapan.com/s/no-loop"
        caption = text

    class Event:
        chat = Chat()
        chat_id = Chat.id
        message = Message()

        async def get_chat(self):
            return self.chat

    summary_queue = asyncio.Queue()
    resource_queue = asyncio.Queue()
    await EventRouter(summary_queue, resource_queue, {str(Chat.id): setting}).route_event(Event())
    assert resource_queue.qsize() == 0
    assert summary_queue.qsize() == 0


@pytest.mark.asyncio
async def test_success_route_is_fixed_and_failure_route_never_uses_source_role():
    notifier = TransferNotifier(
        bot_token="123456:FAKE_TOKEN",
        admin_tg_id=8586984520,
        default_channel_id="-1000000000000",
        success_chat="@guangyazhauncun",
    )
    success_response = httpx.Response(200, json={"ok": True, "result": {"message_id": 501}}, request=httpx.Request("POST", "https://example.com"))
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as post:
        post.return_value = success_response
        result = await notifier.notify_success_result(
            task_payload={"task_id": 1301, "title": "解垢", "season": 1, "episode_keys": ["S01E03"], "source_channel_id": "framehdr"},
            transfer_result={"verified": True, "remote_files": ["S01E03-Gyy.mkv"]},
        )
        assert result.sent is True
        assert result.target_chat_id == "@guangyazhauncun"
        assert result.target_source == "transfer_success_chat"
        assert post.call_args.kwargs["json"]["chat_id"] == "@guangyazhauncun"

        failure = await notifier.notify_failure_result(
            task_payload={"title": "失败", "source_channel_id": "framehdr", "requester_chat_id": "framehdr"},
            error_message="provider returned code 127",
            task_id=99,
            category="NETWORK_ERROR",
            stage="restore",
        )
        assert failure.sent is True
        assert failure.target_chat_id == 8586984520
        assert failure.target_source != "source_channel_id"
        assert "未知错误" not in post.call_args.kwargs["json"]["text"]


@pytest.mark.asyncio
async def test_route_verifier_classifies_bot_membership_and_rights():
    verifier = TelegramRouteVerifier(bot_token="token", bot_username="zhuixin001_bot")

    async def fake_get(method, params=None):
        if method == "getMe":
            return 200, {"ok": True, "result": {"id": 8769755152, "username": "zhuixin001_bot"}}
        if method == "getChat":
            return 200, {"ok": True, "result": {"id": -1001, "type": "channel", "username": params["chat_id"].lstrip("@")}}
        return 200, {"ok": True, "result": {"status": "administrator", "can_post_messages": True, "can_edit_messages": True}}

    verifier._get = fake_get
    result = await verifier.verify(["@guangyazhauncun", "@guangyaziyuanfenxiang"])
    assert result["identity_ok"] is True
    assert result["all_channels_valid"] is True
    assert all(row["validation_code"] == "OK" for row in result["channels"].values())


def test_privileged_callbacks_have_server_side_authorization_boundary():
    source = Path("app/bot/main.py").read_text()
    for callback in ("tx_fail_retry", "tx_fail_switch", "tx_fail_rescout", "tx_fail_cancel", "tx_fail_ignore", "queue_failed", "menu:transfer_resume", "admin:confirm_add"):
        assert callback in source
    assert "_authorize_event(call" in source
