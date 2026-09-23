import pytest

from app.transfer.adapters.guangya import GuangyaAdapter


class NestedCompletedRootAdapter(GuangyaAdapter):
    def __init__(self):
        super().__init__(write_enabled=False)
        self.items={
            "completed-root": [{"fileId":"tv-root","name":"电视剧","resType":2}],
            "tv-root": [{"fileId":"country","name":"日韩剧","resType":2}],
            "country": [{"fileId":"series","name":"剧名 (2024) {tmdbid-73456}【完结】","resType":2}],
        }

    async def post(self, client, url, payload, headers):
        if url.endswith('/file/get_file_list'):
            return {"code":0,"data":{"list":self.items.get(str(payload.get('parentId')),[]),"hasMore":False}}
        raise AssertionError(url)
@pytest.mark.asyncio
async def test_promotion_conflict_scan_detects_same_tmdb_in_ongoing_and_completed():
    adapter=NestedCompletedRootAdapter()
    adapter.items.update({
        "ongoing-root": [{"fileId":"ongoing-tv-root","name":"电视剧","resType":2}],
        "ongoing-tv-root": [{"fileId":"ongoing-category","name":"国产剧","resType":2}],
        "ongoing-category": [{"fileId":"ongoing-series","name":"剧名 {tmdbid-73456}","resType":2}],
    })

    result=await adapter.inspect_completed_root_conflict_readonly(
        auth_token="x",
        completed_root_id="completed-root",
        ongoing_root_id="ongoing-root",
        tmdb_id=73456,
        title="剧名",
        media_root="电视剧",
    )

    assert result["status"] == "DUPLICATE_TMDB_ROOT"
    assert result["conflict"] is True
    assert {root["kind"] for root in result["series_roots"]} == {"ongoing", "completed"}


@pytest.mark.asyncio
async def test_completed_root_conflict_check_finds_series_below_media_and_category_folders():
    adapter=NestedCompletedRootAdapter()

    result=await adapter.inspect_completed_root_conflict_readonly(
        auth_token="x",
        completed_root_id="completed-root",
        tmdb_id=73456,
        title="剧名",
        media_root="电视剧",
    )

    assert result["status"] == "VERIFIED"
    assert result["conflict"] is True
    assert result["series_roots"] == [
        {"folder_id":"series","name":"剧名 (2024) {tmdbid-73456}【完结】","parent_id":"country","path":"影视转存总目录/电视剧/日韩剧/剧名 (2024) {tmdbid-73456}【完结】","kind":"completed"}
    ]
