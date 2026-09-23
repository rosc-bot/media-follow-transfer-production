import pytest

from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.errors import FileSelectionError
from app.transfer.guangya_auth import context_from_auth_ref


class LayoutAdapter(GuangyaAdapter):
    def __init__(self, children):
        super().__init__(write_enabled=False)
        self.children = children
        self.write_calls = []

    async def post(self, client, url, payload, headers):
        if url.endswith("/file/get_file_list"):
            return {"code": 0, "data": {"list": list(self.children.get(str(payload.get("parentId")), [])), "hasMore": False}}
        if url.endswith("/file/create_dir"):
            self.write_calls.append(("create_dir", dict(payload)))
            parent = str(payload["parentId"])
            folder_id = f"created-{len(self.write_calls)}"
            self.children.setdefault(parent, []).append({"fileId": folder_id, "name": payload["dirName"], "resType": 2})
            self.children.setdefault(folder_id, [])
            return {"code": 0, "data": {}}
        raise AssertionError(f"unexpected endpoint {url}")


async def _prepare(adapter, payload):
    return await adapter._prepare_destination_layout(
        object(), payload=payload, root_id=str(payload["target_folder_id"]),
        ctx=context_from_auth_ref("access-token"),
    )


def _payload(*, target="ongoing-root", season_name: str | None="S02", destination_kind="ongoing"):
    return {
        "target_folder_id": target,
        "ongoing_root_id": "ongoing-root",
        "completed_root_id": "completed-root",
        "tmdb_id": 73456,
        "media_type": "tv",
        "destination_kind": destination_kind,
        "series_folder_name": "剧名 (2024) {tmdbid-73456}",
        "season_folder_name": season_name,
        "season": 2 if season_name else 1,
    }


@pytest.mark.asyncio
async def test_reuses_chinese_second_season_instead_of_creating_s02():
    adapter = LayoutAdapter({
        "ongoing-root": [{"fileId": "series", "name": "剧名 (2024) {tmdbid-73456}", "resType": 2}],
        "completed-root": [],
        "series": [{"fileId": "season-cn-2", "name": "第二季", "resType": 2}],
        "season-cn-2": [{"fileId": "e01", "name": "S02E01.mkv", "size": 123, "resType": 1}],
    })

    series_id, season_id = await _prepare(adapter, _payload())

    assert (series_id, season_id) == ("series", "season-cn-2")
    assert adapter.write_calls == []
    assert adapter.children["series"] == [{"fileId": "season-cn-2", "name": "第二季", "resType": 2}]


@pytest.mark.asyncio
async def test_duplicate_season_identity_fails_before_creating_another_folder():
    adapter = LayoutAdapter({
        "ongoing-root": [{"fileId": "series", "name": "剧名 (2024) {tmdbid-73456}", "resType": 2}],
        "completed-root": [],
        "series": [
            {"fileId": "season-s02", "name": "S02", "resType": 2},
            {"fileId": "season-cn-2", "name": "第二季", "resType": 2},
        ],
    })

    with pytest.raises(FileSelectionError) as exc:
        await _prepare(adapter, _payload())
    assert exc.value.code == "DUPLICATE_SEASON_ROOT"

    assert adapter.write_calls == []


@pytest.mark.asyncio
async def test_completed_only_root_is_reused_for_later_missing_episode():
    adapter = LayoutAdapter({
        "ongoing-root": [],
        "completed-root": [{"fileId": "completed-series", "name": "剧名 (2024) {tmdbid-73456}【完结】", "resType": 2}],
        "completed-series": [{"fileId": "season-cn-2", "name": "第二季", "resType": 2}],
        "season-cn-2": [{"fileId": "e01", "name": "S02E01.mkv", "size": 123, "resType": 1}],
    })

    series_id, season_id = await _prepare(adapter, _payload())

    assert (series_id, season_id) == ("completed-series", "season-cn-2")
    assert adapter.write_calls == []
    assert adapter.children["ongoing-root"] == []
    assert adapter.children["completed-root"][0]["fileId"] == "completed-series"
    assert adapter.children["completed-root"][0]["name"].endswith("【完结】")
    assert adapter.children["season-cn-2"][0]["name"] == "S02E01.mkv"


@pytest.mark.asyncio
async def test_same_tmdb_roots_in_both_lifecycle_roots_fail_closed():
    adapter = LayoutAdapter({
        "ongoing-root": [{"fileId": "ongoing-series", "name": "剧名 (2024) {tmdbid-73456}", "resType": 2}],
        "completed-root": [{"fileId": "completed-series", "name": "剧名 (2024) {tmdbid-73456}【完结】", "resType": 2}],
    })

    with pytest.raises(FileSelectionError) as exc:
        await _prepare(adapter, _payload())
    assert exc.value.code == "DUPLICATE_TMDB_ROOT"

    assert adapter.write_calls == []


@pytest.mark.asyncio
async def test_single_season_upgrade_reuses_existing_first_season_folder():
    adapter = LayoutAdapter({
        "ongoing-root": [{"fileId": "series", "name": "剧名 (2024) {tmdbid-73456}", "resType": 2}],
        "completed-root": [],
        "series": [{"fileId": "season-cn-1", "name": "第一季", "resType": 2}],
        "season-cn-1": [{"fileId": "old-e01", "name": "E01.mkv", "size": 456, "resType": 1}],
    })

    series_id, season_id = await _prepare(adapter, _payload(season_name=None))

    assert (series_id, season_id) == ("series", "season-cn-1")
    assert adapter.write_calls == []
    assert adapter.children["series"] == [{"fileId": "season-cn-1", "name": "第一季", "resType": 2}]


@pytest.mark.asyncio
async def test_mixed_root_layout_does_not_create_missing_s02():
    adapter = LayoutAdapter({
        "ongoing-root": [{"fileId": "series", "name": "剧名 (2024) {tmdbid-73456}", "resType": 2}],
        "completed-root": [],
        "series": [
            {"fileId": "season-cn-1", "name": "第一季", "resType": 2},
            {"fileId": "loose-e02", "name": "S01E02.mkv", "size": 456, "resType": 1},
        ],
    })

    with pytest.raises(FileSelectionError) as exc:
        await _prepare(adapter, _payload(season_name="S02"))
    assert exc.value.code == "MIXED_SINGLE_SEASON_LAYOUT"

    assert adapter.write_calls == []
    assert len(adapter.children["series"]) == 2


@pytest.mark.asyncio
async def test_flat_root_files_do_not_get_a_new_season_child_during_upgrade():
    adapter = LayoutAdapter({
        "ongoing-root": [{"fileId": "series", "name": "剧名 (2024) {tmdbid-73456}", "resType": 2}],
        "completed-root": [],
        "series": [{"fileId": "loose-e01", "name": "S01E01.mkv", "size": 456, "resType": 1}],
    })

    with pytest.raises(FileSelectionError) as exc:
        await _prepare(adapter, _payload(season_name="S02"))
    assert exc.value.code == "MIXED_SINGLE_SEASON_LAYOUT"

    assert adapter.write_calls == []
    assert adapter.children["series"] == [{"fileId": "loose-e01", "name": "S01E01.mkv", "size": 456, "resType": 1}]


@pytest.mark.asyncio
async def test_untagged_same_display_title_blocks_creation_as_identity_unverified():
    adapter = LayoutAdapter({
        "ongoing-root": [],
        "completed-root": [{"fileId": "tv-root", "name": "电视剧", "resType": 2}],
        "tv-root": [{"fileId": "category", "name": "国产剧", "resType": 2}],
        "category": [{"fileId": "untagged", "name": "剧名 (2024) 4K", "resType": 2}],
    })
    payload = _payload()
    payload["media_root"] = "电视剧"
    payload["media_category"] = "国产剧"

    with pytest.raises(FileSelectionError) as exc:
        await _prepare(adapter, payload)
    assert exc.value.code == "SERIES_ROOT_IDENTITY_UNVERIFIED"

    assert adapter.write_calls == []
