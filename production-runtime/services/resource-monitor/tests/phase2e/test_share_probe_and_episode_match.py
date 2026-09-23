"""Phase 2E: read-only Guangya share probing and explicit episode matching."""

from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_share_probe_recurses_into_nested_directories_and_keeps_only_videos():
    from app.transfer.share_probe import GuangyaShareProbe

    adapter = AsyncMock()
    adapter.inspect_share.return_value = {
        "share_accessible": True,
        "share_id": "share-1",
        "items": 4,
        "directories": 2,
        "files": [
            {"name": "Show/Season 01/Show.S01E101.mkv", "path": "Show/Season 01/Show.S01E101.mkv"},
            {"name": "Show/Season 01/Show.S01E101.ass", "path": "Show/Season 01/Show.S01E101.ass"},
        ],
        "video_files": [
            {"name": "Show/Season 01/Show.S01E101.mkv", "path": "Show/Season 01/Show.S01E101.mkv"},
        ],
        "errors": [],
        "error_code": "SHARE_VALID",
    }
    probe = GuangyaShareProbe(adapter=adapter, max_depth=3, max_items=100, max_pages=10)

    report = await probe.probe("https://pan.guangyapan.com/s/share-1")

    assert report["share_accessible"] is True
    assert report["video_count"] == 1
    assert report["video_names"] == ["Show/Season 01/Show.S01E101.mkv"]
    assert report["directories"] == 2
    assert report["error_code"] == "SHARE_VALID"
    adapter.inspect_share.assert_awaited_once()


def test_episode_matcher_supports_three_digit_and_known_season_short_forms():
    from app.transfer.episode_matcher import diagnose_episode_match

    rows = diagnose_episode_match(
        target_episode_key="S01E101",
        known_season=1,
        video_names=[
            "Show.S01E101.1080p.mkv",
            "Show.E101.mp4",
            "剧名.第101集.mkv",
            "101.mp4",
        ],
    )

    assert [row["match_result"] for row in rows] == ["MATCH", "MATCH", "MATCH", "REJECT"]
    assert rows[0]["detected_season"] == 1 and rows[0]["detected_episode"] == 101
    assert rows[1]["detected_season"] == 1 and rows[1]["detected_episode"] == 101
    assert rows[2]["detected_season"] == 1 and rows[2]["detected_episode"] == 101
    assert rows[3]["reject_reason"] == "BARE_NUMBER_UNSUPPORTED"


def test_episode_matcher_never_treats_collection_or_special_as_one_episode():
    from app.transfer.episode_matcher import diagnose_episode_match

    rows = diagnose_episode_match(
        target_episode_key="S01E07",
        known_season=1,
        video_names=["剧名.全集.mkv", "剧名.S01.SP.mkv", "剧名.S01E07.PV.mp4", "剧名.S01E07.mkv"],
    )

    assert [row["match_result"] for row in rows] == ["REVIEW", "REVIEW", "REVIEW", "MATCH"]
    assert rows[0]["reject_reason"] == "COLLECTION_NOT_SINGLE_EPISODE"
    assert rows[1]["reject_reason"] == "SPECIAL_NOT_REGULAR_EPISODE"


@pytest.mark.asyncio
async def test_probe_timeout_is_not_invalid_share():
    from app.transfer.share_probe import GuangyaShareProbe

    adapter = AsyncMock()
    adapter.inspect_share.side_effect = TimeoutError("read timed out")
    report = await GuangyaShareProbe(adapter=adapter).probe("https://pan.guangyapan.com/s/x")

    assert report["share_accessible"] is False
    assert report["error_code"] == "NETWORK_TIMEOUT"
    assert report["deterministic_failure"] is False
