import pytest

from app.transfer.adapters.guangya import GuangyaAdapter


class ScriptedGuangyaAdapter(GuangyaAdapter):
    def __init__(self):
        super().__init__(write_enabled=True)
        self.calls = []
        self.target_reads = 0

    async def post(self, client, url, payload, headers):
        self.calls.append((url, payload))
        if url.endswith('/get_share_access_token'):
            return {'code': 0, 'data': {'accessToken': 'share-token'}}
        if url.endswith('/get_share_page_files_list'):
            if payload['page'] == 0:
                return {'code': 0, 'data': {'list': [{'fileId': '1', 'name': 'one.mkv', 'resType': 1}], 'hasMore': True}}
            return {'code': 0, 'data': {'list': [{'fileId': '2', 'name': 'two.mkv', 'resType': 1}], 'hasMore': False}}
        if url.endswith('/restore_share'):
            return {'code': 0, 'data': {}}
        if url.endswith('/file/get_file_list'):
            self.target_reads += 1
            files = [] if self.target_reads == 1 else [
                {'fileId': 'target-one', 'name': 'one.mkv', 'resType': 1},
                {'fileId': 'target-two', 'name': 'two.mkv', 'resType': 1},
            ]
            return {'code': 0, 'data': {'list': files, 'hasMore': False}}
        raise AssertionError(url)


@pytest.mark.asyncio
async def test_guangya_adapter_paginates_share_and_polls_readback_until_verified():
    adapter = ScriptedGuangyaAdapter()

    outcome = await adapter.transfer({
        'share_url': 'https://pan.guangyapan.com/s/share',
        'target_folder_id': 'target',
        'auth_token': 'access-token',
        'expected_files': ['one.mkv', 'two.mkv'],
        'verify_attempts': 2,
        'verify_interval_seconds': 0,
    })

    share_pages = [payload['page'] for url, payload in adapter.calls if url.endswith('/get_share_page_files_list')]
    assert outcome.success is True and outcome.verified is True
    assert outcome.remote_files == ('one.mkv', 'two.mkv')
    assert share_pages == [0, 1]
    assert adapter.target_reads == 3


class NestedFolderGuangyaAdapter(GuangyaAdapter):
    def __init__(self):
        super().__init__(write_enabled=True)
        self.calls = []

    async def post(self, client, url, payload, headers):
        self.calls.append((url, payload))
        if url.endswith('/get_share_access_token'):
            return {'code': 0, 'data': {'accessToken': 'share-token'}}
        if url.endswith('/get_share_page_files_list'):
            parent_id = payload.get('parentId')
            if parent_id is None:
                # Root folder: contains a folder "Season 1"
                return {'code': 0, 'data': {'list': [{'fileId': 'dir-s01', 'name': 'Season 1', 'resType': 2}], 'hasMore': False}}
            if parent_id == 'dir-s01':
                # Nested folder: contains episode files
                return {'code': 0, 'data': {'list': [
                    {'fileId': 'ep1', 'name': 'S01E01.mkv', 'resType': 1},
                    {'fileId': 'ep2', 'name': 'S01E02.mkv', 'resType': 1},
                ], 'hasMore': False}}
            return {'code': 0, 'data': {'list': [], 'hasMore': False}}
        if url.endswith('/restore_share'):
            return {'code': 0, 'data': {}}
        if url.endswith('/file/get_file_list'):
            return {'code': 0, 'data': {'list': [
                {'fileId': 'target-ep1', 'name': 'S01E01.mkv', 'resType': 1},
                {'fileId': 'target-ep2', 'name': 'S01E02.mkv', 'resType': 1},
            ], 'hasMore': False}}
        raise AssertionError(url)


@pytest.mark.asyncio
async def test_guangya_adapter_recursively_discovers_nested_share_files():
    adapter = NestedFolderGuangyaAdapter()
    outcome = await adapter.transfer({
        'share_url': 'https://pan.guangyapan.com/s/nested-share',
        'target_folder_id': 'target-folder',
        'auth_token': 'access-token',
        'expected_files': ['S01E01.mkv', 'S01E02.mkv'],
        'verify_attempts': 1,
    })
    assert outcome.success is True
    assert outcome.verified is True
    assert outcome.remote_files == ('S01E01.mkv', 'S01E02.mkv')
    restore_calls = [p for u, p in adapter.calls if u.endswith('/restore_share')]
    assert len(restore_calls) == 1
    assert set(restore_calls[0]['fileIds']) == {'ep1', 'ep2'}

