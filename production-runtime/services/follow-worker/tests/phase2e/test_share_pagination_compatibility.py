"""Regression: public share API can repeat pages despite no explicit hasMore."""

import pytest

from app.transfer.adapters.guangya import GuangyaAdapter


class RepeatedSharePageAdapter(GuangyaAdapter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pages = []

    async def post(self, client, url, payload, headers):
        del client, headers
        if url.endswith("/get_share_access_token"):
            return {"code": 0, "data": {"accessToken": "share-token"}}
        if url.endswith("/get_share_page_files_list"):
            self.pages.append(payload["page"])
            # 20 exact results induce the defensive page-1 probe. Live public
            # API sometimes repeats page 0 then, without hasMore.
            files = [
                {"fileId": str(index), "name": f"Show.S01E{index:02d}.mkv", "resType": 1}
                for index in range(1, 21)
            ]
            return {"code": 0, "data": {"total": 20, "list": files}}
        raise AssertionError(url)


@pytest.mark.asyncio
async def test_share_probe_stops_on_repeated_implicit_page_without_failing_share():
    adapter = RepeatedSharePageAdapter(write_enabled=False)

    inspected = await adapter.inspect_share(share_url="https://pan.guangyapan.com/s/repeated")

    assert inspected["share_accessible"] is True
    assert len(inspected["video_names"]) == 20
    assert adapter.pages == [0, 1]
