import pytest

from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.errors import TransferErrorCategory
from app.transfer.file_selection import (
    FileSelectionError,
    SelectionMode,
    assert_selection_scope,
    select_files,
)


def _share_files():
    files = []
    for episode in range(1, 9):
        file_id = f"file-{episode}"
        name = f"S01E{episode:02d}-Gyy.mkv"
        for _ in range(2):
            files.append({"fileId": file_id, "name": name, "resType": 1})
    return files


def test_single_episode_empty_expected_files_selects_exact_episode_only():
    result = select_files(
        _share_files(),
        episode_keys=["S01E03"],
        season=1,
        expected_files=[],
    )

    assert result.selection_mode is SelectionMode.SINGLE_EPISODE
    assert result.raw_listing_count == 16
    assert result.share_video_count == 16
    assert result.unique_video_count == 8
    assert result.matched_listing_count == 2
    assert result.matched_unique_file_count == 1
    assert result.selected_file_ids == ["file-3"]
    assert result.selected_file_names == ["S01E03-Gyy.mkv"]
    assert result.decision == "EXACT_SINGLE_EPISODE"


def test_duplicate_listing_is_deduplicated_before_matching():
    result = select_files(_share_files(), episode_keys=["S01E03"], season=1)

    assert result.matched_listing_count == 2
    assert result.matched_unique_file_count == 1
    assert result.selected_file_ids == ["file-3"]


def test_single_episode_zero_match_is_episode_mismatch():
    result = select_files(_share_files(), episode_keys=["S01E99"], season=1)

    assert result.selected_file_ids == []
    assert result.decision == "EPISODE_MISMATCH"
    with pytest.raises(FileSelectionError) as excinfo:
        assert_selection_scope(result)
    assert excinfo.value.code == "EPISODE_MISMATCH"
    assert excinfo.value.category == TransferErrorCategory.EPISODE_MISMATCH


def test_multiple_unique_matches_require_file_selection_review():
    files = _share_files()
    files.append({"fileId": "file-3-hdr", "name": "S01E03-2160p-HDR.mkv", "resType": 1})

    result = select_files(files, episode_keys=["S01E03"], season=1)

    assert result.matched_unique_file_count == 2
    assert result.selected_file_ids == []
    assert result.decision == "FILE_SELECTION_REVIEW"
    with pytest.raises(FileSelectionError) as excinfo:
        assert_selection_scope(result)
    assert excinfo.value.code == "FILE_SELECTION_REVIEW"


def test_explicit_whole_share_selects_all_unique_videos():
    result = select_files(_share_files(), selection_mode=SelectionMode.WHOLE_SHARE)

    assert result.selection_mode is SelectionMode.WHOLE_SHARE
    assert result.unique_video_count == 8
    assert len(result.selected_file_ids) == 8
    assert result.decision == "WHOLE_SHARE"
    assert_selection_scope(result)


def test_collection_requires_explicit_file_names_and_does_not_infer_whole_share():
    result = select_files(
        _share_files(),
        selection_mode=SelectionMode.COLLECTION,
        expected_files=["S01E03-Gyy.mkv", "S01E04-Gyy.mkv"],
    )

    assert result.selected_file_names == ["S01E03-Gyy.mkv", "S01E04-Gyy.mkv"]
    assert len(result.selected_file_ids) == 2
    assert result.decision == "COLLECTION"


def test_unknown_selection_mode_fails_closed():
    with pytest.raises(FileSelectionError) as excinfo:
        select_files(_share_files(), selection_mode="MAGIC_ALL")

    assert excinfo.value.code == "UNKNOWN_SELECTION_MODE"


def test_single_episode_scope_assertion_rejects_more_than_one_file_id():
    result = select_files(_share_files(), episode_keys=["S01E03"], season=1)
    result.selected_file_ids = ["file-3", "file-4"]
    result.selected_file_names = ["S01E03-Gyy.mkv", "S01E04-Gyy.mkv"]

    with pytest.raises(FileSelectionError) as excinfo:
        assert_selection_scope(result)

    assert excinfo.value.code == "CANARY_ABORTED_SELECTION_TOO_BROAD"


class SelectionAdapter(GuangyaAdapter):
    def __init__(self, *, readback_files):
        super().__init__(write_enabled=True)
        self.calls = []
        self.readback_files = readback_files
        self.restore_calls = []
        self.readback_call_count = 0

    async def post(self, client, url, payload, headers):
        self.calls.append((url, payload))
        if url.endswith("/get_share_access_token"):
            return {"code": 0, "data": {"accessToken": "share-token"}}
        if url.endswith("/get_share_page_files_list"):
            return {"code": 0, "data": {"list": _share_files(), "hasMore": False}}
        if url.endswith("/file/get_file_list"):
            parent_id = str(payload.get("parentId") or "")
            if parent_id == "target":
                self.readback_call_count += 1
                items = [] if self.readback_call_count == 1 else self.readback_files
                return {"code": 0, "data": {"list": items, "hasMore": False}}
            return {"code": 0, "data": {"list": [], "hasMore": False}}
        if url.endswith("/restore_share"):
            self.restore_calls.append(payload)
            return {"code": 0, "data": {}}
        raise AssertionError(url)


def _transfer_payload(**overrides):
    payload = {
        "share_url": "https://pan.guangyapan.com/s/share",
        "target_folder_id": "target",
        "auth_token": "access-token",
        "episode_keys": ["S01E03"],
        "season": 1,
        "expected_files": [],
        "verify_attempts": 1,
        "verify_interval_seconds": 0,
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
async def test_adapter_restore_receives_only_hydrated_single_episode_file_id():
    adapter = SelectionAdapter(readback_files=[{"fileId": "file-3", "name": "S01E03-Gyy.mkv", "resType": 1}])

    outcome = await adapter.transfer(_transfer_payload())

    assert outcome.success is True
    assert adapter.restore_calls == [
        {"accessToken": "share-token", "fileIds": ["file-3"], "parentId": "target"}
    ]


@pytest.mark.asyncio
async def test_adapter_rejects_new_extra_episode_as_scope_violation():
    adapter = SelectionAdapter(
        readback_files=[
            {"fileId": "file-3", "name": "S01E03-Gyy.mkv", "resType": 1},
            {"fileId": "file-4", "name": "S01E04-Gyy.mkv", "resType": 1},
        ]
    )

    with pytest.raises(FileSelectionError) as excinfo:
        await adapter.transfer(_transfer_payload())

    assert excinfo.value.code == "TRANSFER_SCOPE_VIOLATION"
    assert adapter.restore_calls[0]["fileIds"] == ["file-3"]


@pytest.mark.asyncio
async def test_adapter_whole_share_requires_explicit_mode_and_restores_unique_files():
    readback = [
        {"fileId": f"file-{episode}", "name": f"S01E{episode:02d}-Gyy.mkv", "resType": 1}
        for episode in range(1, 9)
    ]
    adapter = SelectionAdapter(readback_files=readback)

    outcome = await adapter.transfer(
        _transfer_payload(selection_mode="WHOLE_SHARE", episode_keys=[], expected_files=[])
    )

    assert outcome.success is True
    assert set(adapter.restore_calls[0]["fileIds"]) == {f"file-{episode}" for episode in range(1, 9)}


@pytest.mark.asyncio
async def test_adapter_unknown_mode_does_not_call_restore():
    adapter = SelectionAdapter(readback_files=[])

    with pytest.raises(FileSelectionError) as excinfo:
        await adapter.transfer(_transfer_payload(selection_mode="MAGIC_ALL"))

    assert excinfo.value.code == "UNKNOWN_SELECTION_MODE"
    assert adapter.restore_calls == []
