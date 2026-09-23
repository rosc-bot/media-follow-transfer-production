"""Regression tests for production TMDB root reuse, batch selection and layout."""

import httpx
import pytest

from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.canonical_destination import (
    CanonicalDestinationBuilder,
    DestinationMetadataIncomplete,
)
from app.transfer.file_selection import (
    FileSelectionError,
    SelectionMode,
    assert_selection_scope,
    select_files,
)
from app.transfer.guangya_auth import context_from_auth_ref
from app.transfer.missing_episode_preflight import (
    AUTO_SAFE,
    NEEDS_REVIEW,
    REJECTED,
    plan_missing_episode_transfer,
)
from app.transfer.rename import build_rename_plan


def _tmdb_metadata(seasons):
    return {
        "id": 223564,
        "media_type": "tv",
        "origin_country": ["JP"],
        "original_language": "ja",
        "genres": [{"id": 16, "name": "Animation"}],
        "seasons": seasons,
    }


def test_single_season_series_layout_is_flat_but_multi_season_is_nested():
    single = CanonicalDestinationBuilder.build(
        metadata=_tmdb_metadata([{"season_number": 1, "episode_count": 36}]),
        tmdb_id=223564,
        media_type="tv",
        title="作品",
        destination_kind="ongoing",
        season=1,
    )
    multi = CanonicalDestinationBuilder.build(
        metadata=_tmdb_metadata([
            {"season_number": 1, "episode_count": 12},
            {"season_number": 2, "episode_count": 12},
        ]),
        tmdb_id=223564,
        media_type="tv",
        title="作品",
        destination_kind="ongoing",
        season=1,
    )

    assert single.season_name is None
    assert single.inventory_prefix == "电视剧/日番/作品 {tmdbid-223564}"
    assert multi.season_name == "S01"
    assert multi.inventory_prefix == "电视剧/日番/作品 {tmdbid-223564}/S01"


def test_tv_destination_fails_closed_when_tmdb_season_list_is_missing():
    metadata = _tmdb_metadata([])
    metadata.pop("seasons")

    with pytest.raises(DestinationMetadataIncomplete, match="seasons"):
        CanonicalDestinationBuilder.build(
            metadata=metadata,
            tmdb_id=223564,
            media_type="tv",
            title="作品",
            destination_kind="ongoing",
            season=1,
        )


def _batch_files():
    return [
        {"fileId": "e01", "name": "Show.S01E01.mkv", "resType": 1},
        {"fileId": "e02", "name": "Show.S01E02.mkv", "resType": 1},
        # A repeated API page/item with the same stable ID must not create a false multi-match.
        {"fileId": "e01", "name": "Show.S01E01.mkv", "resType": 1},
        {"fileId": "e03", "name": "Show.S01E03.mkv", "resType": 1},
    ]


def test_missing_episode_selection_selects_only_exact_requested_episode_map():
    result = select_files(
        _batch_files(),
        selection_mode=SelectionMode.MISSING_EPISODES,
        episode_keys=["S01E01", "S01E03"],
        season=1,
    )

    assert result.selection_mode is SelectionMode.MISSING_EPISODES
    assert result.selected_file_ids == ["e01", "e03"]
    assert result.selected_file_names == ["Show.S01E01.mkv", "Show.S01E03.mkv"]
    assert result.selected_episode_keys == ["S01E01", "S01E03"]
    assert result.episode_file_map == {"S01E01": "e01", "S01E03": "e03"}
    assert result.decision == "MISSING_EPISODES"
    assert_selection_scope(result)


def test_missing_episode_selection_rejects_unresolved_or_multi_match_episodes():
    duplicate = _batch_files() + [
        {"fileId": "e03-alt", "name": "Show.S01E03.2160p.mkv", "resType": 1},
    ]
    result = select_files(
        duplicate,
        selection_mode=SelectionMode.MISSING_EPISODES,
        episode_keys=["S01E01", "S01E03"],
        season=1,
    )

    assert result.decision == "FILE_SELECTION_REVIEW"
    assert result.selected_file_ids == []
    with pytest.raises(FileSelectionError):
        assert_selection_scope(result)


def test_missing_episode_selection_refuses_partial_episode_coverage():
    result = select_files(
        _batch_files(),
        selection_mode=SelectionMode.MISSING_EPISODES,
        episode_keys=["S01E01", "S01E09"],
        season=1,
    )

    assert result.decision == "EPISODE_MISMATCH"
    assert result.selected_file_ids == []
    with pytest.raises(FileSelectionError):
        assert_selection_scope(result)


class _SeriesRootAdapter(GuangyaAdapter):
    def __init__(self, existing_series):
        super().__init__(write_enabled=True)
        self.calls = []
        self.tree = {
            "root": [{"fileId": "media", "name": "电视剧", "resType": 2}],
            "completed-root": [],
            "media": [{"fileId": "category", "name": "日番", "resType": 2}],
            "category": existing_series,
        }

    async def post(self, client, url, payload, headers):
        self.calls.append((url, dict(payload)))
        if url.endswith("/file/get_file_list"):
            parent = str(payload.get("parentId") or "")
            return {"code": 0, "data": {"list": list(self.tree.get(parent, [])), "hasMore": False}}
        if url.endswith("/file/create_dir"):
            parent = str(payload["parentId"])
            name = str(payload["dirName"])
            file_id = "new-series" if parent == "category" else "new-season"
            self.tree.setdefault(parent, []).append({"fileId": file_id, "name": name, "resType": 2})
            self.tree.setdefault(file_id, [])
            return {"code": 0, "data": {"fileId": file_id}}
        raise AssertionError(url)


async def _prepare_series(adapter):
    async with httpx.AsyncClient() as client:
        return await adapter._prepare_destination_layout(
            client,
            payload={
                "series_folder_name": "Canonical title {tmdbid-223564}",
                "season_folder_name": None,
                "media_root": "电视剧",
                "media_category": "日番",
                "media_type": "tv",
                "tmdb_id": 223564,
                "destination_kind": "ongoing",
                "ongoing_root_id": "root",
                "completed_root_id": "completed-root",
            },
            root_id="root",
            ctx=context_from_auth_ref("access-token"),
        )


@pytest.mark.asyncio
async def test_tmdb_series_root_is_reused_by_identity_even_when_title_differs():
    adapter = _SeriesRootAdapter([
        {"fileId": "existing", "name": "Historical title (2023) 4K {tmdbid-223564}", "resType": 2},
    ])

    series_id, target_id = await _prepare_series(adapter)

    assert series_id == target_id == "existing"
    assert not any(url.endswith("/file/create_dir") for url, _ in adapter.calls)


@pytest.mark.asyncio
async def test_multiple_directories_with_same_tmdb_identity_fail_closed():
    adapter = _SeriesRootAdapter([
        {"fileId": "series-a", "name": "Title A {tmdbid-223564}", "resType": 2},
        {"fileId": "series-b", "name": "Title B (2023) 4K {tmdbid-223564}", "resType": 2},
    ])

    with pytest.raises(FileSelectionError) as exc:
        await _prepare_series(adapter)
    assert exc.value.code == "DUPLICATE_TMDB_ROOT"
    assert not any(url.endswith("/file/create_dir") for url, _ in adapter.calls)


@pytest.mark.asyncio
async def test_readonly_tmdb_root_inspection_reports_all_duplicate_direct_children():
    adapter = _SeriesRootAdapter([
        {"fileId": "series-a", "name": "Title A {tmdbid-223564}", "resType": 2},
        {"fileId": "series-b", "name": "Title B (2023) 4K {tmdbid-223564}", "resType": 2},
    ])

    report = await adapter.inspect_tmdb_series_root_readonly(
        auth_token="access-token",
        target_root_id="root",
        media_root_name="电视剧",
        media_category_name="日番",
        tmdb_id=223564,
    )

    assert report["status"] == "DUPLICATE_TMDB_ROOT"
    assert {row["folder_id"] for row in report["series_roots"]} == {"series-a", "series-b"}


def test_batch_rename_uses_each_verified_episode_identity():
    plan = build_rename_plan(
        [
            {"file_id": "e01", "name": "Show.S01E01.1080p.WEB-DL.mkv"},
            {"file_id": "e03", "name": "Show.S01E03.1080p.WEB-DL.mkv"},
        ],
        selected_file_ids=["e01", "e03"],
        title="作品",
        tmdb_id=223564,
        season=1,
        episode_key="S01E01",
        episode_keys_by_file_id={"e01": "S01E01", "e03": "S01E03"},
        content_complete=False,
        media_type="tv",
    )

    targets = {operation.file_id: operation.new_name for operation in plan.operations}
    assert ".S01E01." in targets["e01"]
    assert ".S01E03." in targets["e03"]


class _BatchTransferAdapter(GuangyaAdapter):
    def __init__(self):
        super().__init__(write_enabled=True)
        self.restore_calls = []
        self.children = {"target": []}
        self.share_files = [
            {"fileId": "e01", "name": "Show.S01E01.1080p.WEB-DL.mkv", "resType": 1, "size": 10},
            {"fileId": "e02", "name": "Show.S01E02.1080p.WEB-DL.mkv", "resType": 1, "size": 11},
            {"fileId": "e03", "name": "Show.S01E03.1080p.WEB-DL.mkv", "resType": 1, "size": 12},
        ]

    async def post(self, client, url, payload, headers):
        if url.endswith("/get_share_access_token"):
            return {"code": 0, "data": {"accessToken": "share-token"}}
        if url.endswith("/get_share_page_files_list"):
            return {"code": 0, "data": {"list": self.share_files, "hasMore": False}}
        if url.endswith("/file/get_file_list"):
            parent = str(payload.get("parentId") or "")
            return {"code": 0, "data": {"list": list(self.children.get(parent, [])), "hasMore": False}}
        if url.endswith("/restore_share"):
            self.restore_calls.append(dict(payload))
            requested = set(payload["fileIds"])
            self.children.setdefault(str(payload["parentId"]), []).extend(
                dict(item) for item in self.share_files if item["fileId"] in requested
            )
            return {"code": 0, "data": {}}
        if url.endswith("/file/rename"):
            for item in self.children.get("target", []):
                if item.get("fileId") == payload["fileId"]:
                    item["name"] = payload["newName"]
            return {"code": 0, "data": {}}
        raise AssertionError(url)


@pytest.mark.asyncio
async def test_worker_adapter_restores_and_renames_all_exact_missing_episodes_once():
    adapter = _BatchTransferAdapter()
    payload = {
        "share_url": "https://pan.guangyapan.com/s/share",
        "target_folder_id": "target",
        "auth_token": "access-token",
        "selection_mode": "MISSING_EPISODES",
        "episode_keys": ["S01E01", "S01E03"],
        "season": 1,
        "tmdb_id": 223564,
        "title": "作品",
        "year": 2023,
        "media_type": "tv",
        "series_status": "Returning Series",
        "destination_kind": "ongoing",
        "verify_attempts": 1,
        "verify_interval_seconds": 0,
    }

    outcome = await adapter.transfer(payload)

    assert outcome.verified is True
    assert adapter.restore_calls == [{
        "accessToken": "share-token",
        "fileIds": ["e01", "e03"],
        "parentId": "target",
    }]
    by_episode = {item["episode_key"]: item["file_name"] for item in outcome.verified_episode_files}
    assert set(by_episode) == {"S01E01", "S01E03"}
    assert ".S01E01." in by_episode["S01E01"]
    assert ".S01E03." in by_episode["S01E03"]


def _presence_share():
    return [
        {"fileId": "e01", "name": "Show.S01E01.mkv", "resType": 1},
        {"fileId": "e02", "name": "Show.S01E02.mkv", "resType": 1},
        {"fileId": "e03", "name": "Show.S01E03.mkv", "resType": 1},
        {"fileId": "e01", "name": "Show.S01E01.mkv", "resType": 1},
    ]


def test_presence_planner_excludes_only_episodes_verified_in_all_ledgers():
    plan = plan_missing_episode_transfer(
        _presence_share(),
        season=1,
        trigger_episode_keys=["S01E01"],
        collected_episode_keys=["S01E01"],
        inventory_episode_keys=[1],
        cloud_episode_keys=["S01E01"],
        cloud_scan_verified=True,
    )

    assert plan.classification == AUTO_SAFE
    assert plan.share_episode_keys == ("S01E01", "S01E02", "S01E03")
    assert plan.missing_episode_keys == ("S01E02", "S01E03")
    assert plan.episode_file_map == {"S01E01": "e01", "S01E02": "e02", "S01E03": "e03"}


def test_presence_planner_repairs_metadata_ledgers_from_verified_cloud_presence():
    plan = plan_missing_episode_transfer(
        _presence_share(),
        season=1,
        trigger_episode_keys=["S01E01"],
        collected_episode_keys=["S01E01"],
        inventory_episode_keys=[],
        cloud_episode_keys=["S01E01"],
        cloud_scan_verified=True,
    )

    assert plan.classification == AUTO_SAFE
    assert plan.reason == "MISSING_EPISODES_CONFIRMED_BY_VERIFIED_CLOUD"
    assert plan.missing_episode_keys == ("S01E02", "S01E03")
    assert plan.presence_decisions["S01E01"]["classification"] == "PRESENT_CONFIRMED"
    assert plan.metadata_reconcile == {"S01E01": ("inventory",)}


def test_presence_planner_fails_closed_on_duplicate_or_unmapped_share_episodes():
    duplicate = _presence_share() + [
        {"fileId": "e01-alt", "name": "Show.S01E01.2160p.mkv", "resType": 1},
    ]
    plan = plan_missing_episode_transfer(
        duplicate,
        season=1,
        trigger_episode_keys=["S01E01"],
        collected_episode_keys=[],
        inventory_episode_keys=[],
        cloud_episode_keys=[],
    )

    assert plan.classification == NEEDS_REVIEW
    assert plan.missing_episode_keys == ()


def test_presence_planner_rejects_already_complete_and_reviews_active_or_completed_conflicts():
    complete = plan_missing_episode_transfer(
        _presence_share(),
        season=1,
        trigger_episode_keys=["S01E01"],
        collected_episode_keys=["S01E01", "S01E02", "S01E03"],
        inventory_episode_keys=[1, 2, 3],
        cloud_episode_keys=["S01E01", "S01E02", "S01E03"],
        cloud_scan_verified=True,
    )
    active = plan_missing_episode_transfer(
        _presence_share(),
        season=1,
        trigger_episode_keys=["S01E01"],
        collected_episode_keys=["S01E01"],
        inventory_episode_keys=[1],
        cloud_episode_keys=["S01E01"],
        cloud_scan_verified=True,
        active_episode_keys=["S01E02"],
    )
    completed = plan_missing_episode_transfer(
        _presence_share(),
        season=1,
        trigger_episode_keys=["S01E01"],
        collected_episode_keys=["S01E01"],
        inventory_episode_keys=[1],
        cloud_episode_keys=["S01E01"],
        cloud_scan_verified=True,
        completed_episode_keys=["S01E02"],
    )

    assert complete.classification == REJECTED
    assert complete.reason == "NO_MISSING_EPISODES"
    assert active.classification == NEEDS_REVIEW
    assert active.reason == "ACTIVE_TRANSFER_OVERLAP:S01E02"
    assert completed.classification == NEEDS_REVIEW
    assert completed.reason == "COMPLETED_TASK_WITHOUT_CLOUD_CLOSURE:S01E02"


@pytest.mark.asyncio
async def test_physical_scan_uses_paginated_readback_without_trusting_sticky_has_more():
    class StickyHasMoreAdapter(GuangyaAdapter):
        def __init__(self):
            super().__init__(write_enabled=False)
            self.calls = []
            self.files = [
                {"fileId": "e01", "name": "Show.S01E01.mkv", "resType": 1},
                {"fileId": "e02", "name": "Show.S01E02.mkv", "resType": 1},
                {"fileId": "e07", "name": "Show.S01E07.mkv", "resType": 1},
            ]

        async def post(self, client, url, payload, headers):
            self.calls.append((url, dict(payload)))
            page = int(payload.get("page") or 0)
            items = self.files if page == 0 else []
            return {"code": 0, "data": {"list": items, "total": 3, "hasMore": True}}

    adapter = StickyHasMoreAdapter()
    scan = await adapter.scan_series_root_readonly(
        auth_token="access-token",
        tmdb_id=223564,
        series_root_id="series-root",
        relevant_seasons=[1],
        timeout_seconds=3,
        max_depth=3,
        max_items=50,
        page_size=100,
        rate_limit_seconds=0,
    )

    assert scan["scan_status"] == "VERIFIED"
    assert scan["cloud_episode_keys_by_season"]["1"] == ["S01E01", "S01E02", "S01E07"]
