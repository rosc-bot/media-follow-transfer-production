"""Phase 2G.4 RED contracts: rename, rich success cards, and promotion gates."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.follow.promotion import (
    evaluate_promotion,
    promotion_readback_decision,
)
from app.transfer.notifier import TransferNotifier, build_success_card
from app.transfer.rename import build_rename_plan, canonical_filename


def test_legacy_canonical_filename_preserves_release_name_and_only_applies_old_cleanup():
    assert canonical_filename("  #追新转存  S01E02-Gyy.mkv  ") == "S01E02-Gyy.mkv"
    assert canonical_filename(
        "The.Rapture.S01E02.Episode.2.2160p.iP.WEB-DL.AAC.2.0.HLG.H.265-BlackTV.mkv"
    ) == "The.Rapture.S01E02.Episode.2.2160p.iP.WEB-DL.AAC.2.0.HLG.H.265-BlackTV.mkv"


def test_conditional_rename_plan_is_scoped_to_selected_verified_file_and_detects_conflict():
    naming = {
        "title": "测试剧",
        "year": 2026,
        "tmdb_id": 1,
        "season": 1,
        "episode_key": "S01E02",
        "media_type": "tv",
        "series_status": "Returning Series",
    }
    plan = build_rename_plan(
        [
            {"fileId": "selected", "name": " #追新转存  S01E02-Gyy.mkv"},
            {"fileId": "other", "name": "S01E01-Gyy.mkv"},
        ],
        selected_file_ids=["selected"],
        destination_kind="ongoing",
        **naming,
    )
    assert plan.status == "RENAME_READY"
    assert plan.decision == "RENAME_STANDARD_CHINESE"
    assert [op.file_id for op in plan.operations] == ["selected"]
    assert plan.operations[0].new_name == "测试剧 (2026) {tmdbid-1}.S01E02.mkv"
    assert plan.operations[0].file_id != "other"

    conflict = build_rename_plan(
        [
            {"fileId": "selected", "name": " #追新转存  S01E02-Gyy.mkv"},
            {"fileId": "other", "name": "测试剧 (2026) {tmdbid-1}.S01E02.mkv"},
        ],
        selected_file_ids=["selected"],
        destination_kind="ongoing",
        **naming,
    )
    assert conflict.status == "RENAME_CONFLICT"
    assert conflict.decision == "RENAME_STANDARD_CHINESE"
    assert not conflict.operations


def test_completed_library_keeps_meaningful_source_names_without_remote_rename():
    plan = build_rename_plan(
        [{"fileId": "f1", "name": "source-release.mkv"}],
        selected_file_ids=["f1"],
        destination_kind="completed",
        lifecycle_verified=True,
        title="测试剧",
        media_type="tv",
        series_status="Ended",
        content_complete=True,
        season=1,
        episode_key="S01E01",
    )
    assert plan.status == "RENAME_SKIPPED_KEEP_EXISTING_NAME"
    assert plan.decision == "KEEP"
    assert plan.operations == ()


@pytest.mark.asyncio
async def test_success_card_is_rich_and_uses_send_photo_with_fixed_success_route():
    notifier = TransferNotifier(bot_token="123456:FAKE_TOKEN", success_chat="@guangyazhauncun")
    payload = {
        "title": "测试剧",
        "season": 1,
        "episode_keys": ["S01E02"],
        "share_url": "https://www.guangyapan.com/s/demo",
        "poster_url": "https://image.tmdb.org/t/p/w500/poster.jpg",
        "series_status": "Ended",
        "total_episodes": 8,
        "collected_episodes": ["S01E01", "S01E02"],
        "version_key": "2160p WEB-DL HLG",
        "source_channel_username": "@source_user",
        "archive_directory": "未完结追新/电视剧/测试剧 {tmdbid-1}/S01",
    }
    result = {"verified": True, "remote_files": ["S01E02-Gyy.mkv"], "selected_file_sizes": {"S01E02-Gyy.mkv": 348400000}}
    card = build_success_card(task_payload=payload, transfer_result=result)
    for text in ("测试剧", "分享链接", "已完结", "收录状态待校验", "本次新增", "规格版本", "资源体积", "归档目录", "智能去重", "@source_user"):
        assert text in card.caption
    assert card.poster_url.endswith("poster.jpg")
    assert "Task" not in card.caption

    response = httpx.Response(200, json={"ok": True, "result": {"message_id": 88}}, request=httpx.Request("POST", "https://example.invalid"))
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as post:
        post.return_value = response
        sent = await notifier.notify_success_result(task_payload=payload, transfer_result=result)
    assert sent.status == "SENT"
    assert post.call_args.args[0].endswith("/sendPhoto")
    assert post.call_args.kwargs["json"]["chat_id"] == "@guangyazhauncun"


@pytest.mark.asyncio
async def test_success_card_falls_back_to_text_when_poster_unavailable():
    notifier = TransferNotifier(bot_token="123456:FAKE_TOKEN", success_chat="@guangyazhauncun")
    payload = {"title": "无海报剧", "season": 1, "episode_keys": ["S01E01"], "poster_url": ""}
    result = {"verified": True, "remote_files": ["S01E01.mkv"]}
    response = httpx.Response(200, json={"ok": True, "result": {"message_id": 89}}, request=httpx.Request("POST", "https://example.invalid"))
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as post:
        post.return_value = response
        sent = await notifier.notify_success_result(task_payload=payload, transfer_result=result)
    assert sent.sent is True
    assert post.call_args.args[0].endswith("/sendMessage")
    assert "POSTER_UNAVAILABLE" in sent.as_dict().get("error", "") or "无海报剧" in post.call_args.kwargs["json"]["text"]


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("A", "PROMOTION_READY"),
        ("B", "CONTENT_INCOMPLETE"),
        ("C", "INVENTORY_INCOMPLETE"),
        ("D", "ACTIVE_TRANSFER"),
        ("E", "SERIES_NOT_READY"),
    ],
)
def test_promotion_gate_scenarios(case, expected):
    base = {
        "tmdb_id": 1,
        "title": "测试剧",
        "series_status": "Ended",
        "ongoing_root": "ongoing-root",
        "completed_root": "completed-root",
        "seasons": [{"season": 1, "total_expected": 8, "collected_count": 8, "inventory_count": 8, "cloud_count": 8, "series_status": "Ended"}],
    }
    if case == "B":
        base["seasons"][0]["collected_count"] = 7
    elif case == "C":
        base["seasons"][0]["inventory_count"] = 7
    elif case == "D":
        base["active_transfer_count"] = 1
    elif case == "E":
        base["seasons"] = [
            {"season": 1, "total_expected": 8, "collected_count": 8, "inventory_count": 8, "cloud_count": 8, "series_status": "Ended"},
            {"season": 2, "total_expected": 10, "collected_count": 10, "inventory_count": 10, "cloud_count": 10, "series_status": "Returning Series"},
        ]
    decision = evaluate_promotion(**base)
    assert decision.decision == expected


def test_promotion_readback_requires_complete_destination_and_empty_source():
    assert promotion_readback_decision(
        move_returned=True,
        expected_files_by_season={"S01": ["S01E01.mkv", "S01E02.mkv"]},
        observed_files_by_season={"S01": ["S01E01.mkv", "S01E02.mkv"]},
        source_root_exists=False,
        destination_conflict=False,
    ) == "PROMOTION_COMPLETED"
    assert promotion_readback_decision(
        move_returned=True,
        expected_files_by_season={"S01": ["S01E01.mkv", "S01E02.mkv"]},
        observed_files_by_season={"S01": ["S01E01.mkv"]},
        source_root_exists=False,
        destination_conflict=False,
    ) == "PROMOTION_UNVERIFIED"