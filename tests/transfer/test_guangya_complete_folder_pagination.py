import pytest

from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.guangya_auth import context_from_auth_ref


class TruncatedDirectoryAdapter(GuangyaAdapter):
    async def post(self, _client, _url, payload, _headers):
        page = int(payload["page"])
        if page == 0:
            items = [{"fileId": f"file-{index}", "name": f"f{index}.mkv", "resType": 1} for index in range(20)]
            return {"code": 0, "data": {"total": 41, "list": items, "hasMore": True}}
        items = [{"fileId": f"file-{index}", "name": f"f{index}.mkv", "resType": 1} for index in range(20, 25)]
        return {"code": 0, "data": {"total": 41, "list": items, "hasMore": False}}


@pytest.mark.asyncio
async def test_folder_listing_fails_closed_on_short_page_before_reported_total():
    adapter = TruncatedDirectoryAdapter(write_enabled=False)

    with pytest.raises(RuntimeError, match="SHORT_PAGE_BEFORE_TOTAL"):
        await adapter._list_folder_items(
            object(), parent_id="library-root", ctx=context_from_auth_ref("access-token")
        )
