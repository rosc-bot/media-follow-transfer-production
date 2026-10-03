import pytest

from app.transfer.adapters.guangya import GuangyaAdapter


class RefreshingGuangyaAdapter(GuangyaAdapter):
    def __init__(self):
        super().__init__(write_enabled=True)
        self.refreshed = None
        self.restore_headers = None

    async def refresh_access_token(self, client, refresh_token):
        self.refreshed = refresh_token
        return 'fresh-access-token'

    async def post(self, client, url, payload, headers):
        if url.endswith('/get_share_access_token'):
            return {'code': 0, 'data': {'accessToken': 'share-token'}}
        if url.endswith('/get_share_page_files_list'):
            return {'code': 0, 'data': {'list': [{'fileId': '1', 'name': 'episode.mkv', 'resType': 1}], 'hasMore': False}}
        if url.endswith('/restore_share'):
            self.restore_headers = headers
            return {'code': 0, 'data': {}}
        if url.endswith('/file/get_file_list'):
            return {'code': 0, 'data': {'list': [{'name': 'episode.mkv', 'resType': 1}], 'hasMore': False}}
        raise AssertionError(url)


@pytest.mark.asyncio
async def test_guangya_adapter_refreshes_access_token_before_restore_when_only_refresh_exists():
    adapter = RefreshingGuangyaAdapter()

    outcome = await adapter.transfer({
        'share_url': 'https://pan.guangyapan.com/s/share',
        'target_folder_id': 'target',
        'auth_token': 'gy.refresh-token',
        'expected_files': ['episode.mkv'],
        'verify_interval_seconds': 0,
    })

    assert outcome.verified is True
    assert adapter.refreshed == 'gy.refresh-token'
    assert adapter.restore_headers.get('authorization') == 'Bearer fresh-access-token'
