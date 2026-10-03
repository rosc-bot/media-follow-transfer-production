"""Phase 2G.5 physical scan and promotion watermark contracts."""

import asyncio

import pytest

from app.follow.physical_cloud_inventory import PhysicalCloudInventoryScanner


@pytest.mark.asyncio
async def test_physical_scan_is_paginated_bounded_and_watermarked():
    calls = []

    async def list_page(parent_id, page, page_size):
        calls.append((parent_id, page, page_size))
        if parent_id == "series" and page == 0:
            return {
                "items": [{"id": "season", "name": "S01", "resType": 2}],
                "hasMore": False,
            }
        if parent_id == "season" and page == 0:
            return {
                "items": [
                    {"id": "e1", "name": "测试剧.S01E01.2160p.mkv", "resType": 1},
                    {"id": "e2", "name": "测试剧.S01E02.2160p.mkv", "resType": 1},
                ],
                "hasMore": False,
            }
        return {"items": [], "hasMore": False}

    scanner = PhysicalCloudInventoryScanner(list_page, page_size=2, rate_limit_seconds=0)
    result = await scanner.scan(tmdb_id=1, series_root_id="series", relevant_seasons=[1])
    assert result.scan_status == "VERIFIED"
    assert result.cloud_episode_keys_by_season == {1: ("S01E01", "S01E02")}
    assert result.file_count == 2
    assert result.scan_watermark and result.scan_watermark.startswith("physical:1:")
    assert calls == [("series", 0, 2), ("season", 0, 2)]


@pytest.mark.asyncio
async def test_full_page_without_pagination_proof_fails_closed():
    async def list_page(_parent_id, _page, _page_size):
        return {"items": [{"id": "e1", "name": "测试剧.S01E01.mkv", "resType": 1}]}

    scanner = PhysicalCloudInventoryScanner(list_page, page_size=1, rate_limit_seconds=0)
    result = await scanner.scan(tmdb_id=1, series_root_id="series", relevant_seasons=[1])
    assert result.scan_status == "PAGINATION_INCOMPLETE"
    assert result.scan_watermark is None


@pytest.mark.asyncio
async def test_timeout_fails_closed_and_cache_is_read_only():
    calls = 0

    async def list_page(_parent_id, _page, _page_size):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.2)
        return {"items": [], "hasMore": False}

    scanner = PhysicalCloudInventoryScanner(
        list_page,
        timeout_seconds=0.001,
        cache_ttl_seconds=60,
        rate_limit_seconds=0,
    )
    timeout_result = await scanner.scan(tmdb_id=1, series_root_id="series", relevant_seasons=[1])
    assert timeout_result.scan_status == "TIMEOUT_UNVERIFIED"
    assert timeout_result.scan_watermark is None

    async def cached_page(_parent_id, _page, _page_size):
        nonlocal calls
        calls += 1
        return {"items": [{"id": "e1", "name": "测试剧.S01E01.mkv", "resType": 1}], "hasMore": False}

    scanner = PhysicalCloudInventoryScanner(cached_page, page_size=10, cache_ttl_seconds=60, rate_limit_seconds=0)
    first = await scanner.scan(tmdb_id=1, series_root_id="series", relevant_seasons=[1])
    second = await scanner.scan(tmdb_id=1, series_root_id="series", relevant_seasons=[1])
    assert first.scan_status == "VERIFIED"
    assert second.from_cache is True
    assert second.scan_watermark == first.scan_watermark


def test_promotion_requires_same_round_verified_cloud_scan_when_requested():
    from app.follow.promotion import evaluate_promotion

    base = {
        "tmdb_id": 7,
        "title": "测试剧",
        "series_status": "Ended",
        "ongoing_root": "ongoing",
        "completed_root": "completed",
        "seasons": [{"season": 1, "total_expected": 2, "collected_count": 2, "inventory_count": 2, "cloud_count": 2, "series_status": "Ended"}],
        "require_cloud_scan_watermark": True,
        "promotion_evaluation_id": "promotion:7:round-1",
    }
    unverified = evaluate_promotion(**base, cloud_scan_status="TIMEOUT_UNVERIFIED")
    assert unverified.decision == "SCAN_UNVERIFIED"
    verified = evaluate_promotion(
        **base,
        cloud_scan_status="VERIFIED",
        cloud_scan_timestamp="2026-09-22T00:00:00+00:00",
        cloud_scan_watermark="physical:7:abc",
    )
    assert verified.decision == "PROMOTION_READY"
    assert verified.promotion_evaluation_id == "promotion:7:round-1"
