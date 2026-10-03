"""Phase 2G.5 notification routing and business-status contracts."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.models.resource import Resource
from app.transfer.notifier import TransferNotifier, build_success_card


def test_business_status_is_not_technical_task_status():
    base = {
        "title": "测试剧",
        "episode_keys": ["S01E02"],
        "series_status": "Returning Series",
        "total_episodes": 2,
        "collected_episodes": ["S01E01"],
    }
    assert build_success_card(task_payload=base, transfer_result={"verified": True}).business_status == "连载更新中"
    base["series_status"] = "Ended"
    assert build_success_card(task_payload=base, transfer_result={"verified": True}).business_status == "已完结，补齐中"
    base.update({"content_complete": True, "inventory_count": 2, "cloud_count": 2})
    assert build_success_card(task_payload=base, transfer_result={"verified": True}).business_status == "已收齐，等待归档"
    assert build_success_card(
        task_payload={**base, "promotion_status": "PROMOTION_COMPLETED"},
        transfer_result={"verified": True},
    ).business_status == "已完结，已归档"


@pytest.mark.asyncio
async def test_success_route_ignores_payload_redirect_and_uses_resource_share_url():
    notifier = TransferNotifier(
        bot_token="123456:FAKE_TOKEN",
        admin_tg_id=8586984520,
        success_chat="@guangyazhauncun",
        publish_chat="@guangyaziyuanfenxiang",
    )
    resource = Resource(
        identity_key="test-notify",
        tmdb_id=123,
        title="测试剧",
        media_type="tv",
        season=1,
        episode=2,
        episode_key="S01E02",
        share_url="https://www.guangyapan.com/s/real-share?access_token=secret",
        source_type="framehdr",
        file_names=["测试剧.S01E02.mkv"],
    )
    response = httpx.Response(
        200,
        json={"ok": True, "result": {"message_id": 1}},
        request=httpx.Request("POST", "https://example.invalid"),
    )
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as post:
        post.return_value = response
        result = await notifier.notify_success_result(
            task_payload={"success_notification_chat": 8586984520},
            transfer_result={
                "verified": True,
                "selected_file_names": ["测试剧.S01E02.mkv"],
                "selected_file_sizes": {"测试剧.S01E02.mkv": 100},
            },
            resource=resource,
        )
    assert result.sent is True
    assert result.target_chat_id == "@guangyazhauncun"
    assert result.target_source == "transfer_success_chat"
    assert post.await_count == 2
    assert [call.kwargs["json"]["chat_id"] for call in post.await_args_list] == [
        "@guangyazhauncun", "@guangyaziyuanfenxiang",
    ]
    body = post.await_args_list[0].kwargs["json"]
    assert body["chat_id"] == "@guangyazhauncun"
    assert "real-share" in body["text"]
    assert all("access_token" not in call.kwargs["json"]["text"] for call in post.await_args_list)
    # The public card must not publish the source owner's share as our own.
    assert "real-share" not in post.await_args_list[1].kwargs["json"]["text"]
