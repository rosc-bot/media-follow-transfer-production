import pytest

from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.errors import PromotionUnverifiedError


class LayoutGuangyaAdapter(GuangyaAdapter):
    def __init__(self):
        super().__init__(write_enabled=True)
        self.calls: list[tuple[str, dict]] = []
        self.children = {
            'completed-root': [],
            'ongoing-root': [],
            'series-1': [],
            'season-1': [],
            'legacy-series': [{'fileId': 'legacy-s01', 'name': 'S01', 'resType': 2}],
            'legacy-s01': [{'fileId': 'old-episode', 'name': 'S01E01.mkv', 'resType': 1}],
        }

    async def post(self, client, url, payload, headers):
        self.calls.append((url, payload))
        if url.endswith('/get_share_access_token'):
            return {'code': 0, 'data': {'accessToken': 'share-token'}}
        if url.endswith('/get_share_page_files_list'):
            return {'code': 0, 'data': {'list': [{'fileId': 'incoming', 'name': 'S01E02.mkv', 'resType': 1}], 'hasMore': False}}
        if url.endswith('/file/get_file_list'):
            parent_id = str(payload.get('parentId') or '')
            return {'code': 0, 'data': {'list': self.children.get(parent_id, []), 'hasMore': False}}
        if url.endswith('/file/create_dir'):
            parent_id = str(payload['parentId'])
            dir_name = payload['dirName']
            file_id = 'series-1' if parent_id in {'ongoing-root', 'completed-root'} else 'season-1'
            self.children.setdefault(parent_id, []).append({'fileId': file_id, 'name': dir_name, 'resType': 2})
            self.children.setdefault(file_id, [])
            return {'code': 0, 'data': {'fileId': file_id}}
        if url.endswith('/file/move_file'):
            assert payload == {'fileIds': ['legacy-series'], 'parentId': 'completed-root'}
            self.children['ongoing-root'] = [
                item for item in self.children.get('ongoing-root', [])
                if item.get('fileId') != 'legacy-series'
            ]
            self.children['completed-root'].append({'fileId': 'legacy-series', 'name': '旧剧集 (2025) {tmdbid-1}', 'resType': 2})
            return {'code': 0, 'data': {}}
        if url.endswith('/file/rename'):
            for parent_items in self.children.values():
                for item in parent_items:
                    if item.get('fileId') == payload['fileId']:
                        item['name'] = payload['newName']
            return {'code': 0, 'data': {}}
        if url.endswith('/restore_share'):
            target = str(payload['parentId'])
            self.children.setdefault(target, []).append({'fileId': 'incoming-remote', 'name': 'S01E02.mkv', 'resType': 1})
            return {'code': 0, 'data': {}}
        raise AssertionError(url)


@pytest.mark.asyncio
async def test_transfer_creates_series_and_season_directories_under_ongoing_root():
    adapter = LayoutGuangyaAdapter()

    outcome = await adapter.transfer({
        'share_url': 'https://pan.guangyapan.com/s/share',
        'target_folder_id': 'ongoing-root',
        'ongoing_root_id': 'ongoing-root',
        'completed_root_id': 'completed-root',
        'tmdb_id': 1,
        'media_type': 'tv',
        'auth_token': 'access-token',
        'expected_files': ['S01E02.mkv'],
        'series_folder_name': '新剧集 (2025) {tmdbid-1}',
        'season_folder_name': 'S01',
        'destination_kind': 'ongoing',
        'verify_attempts': 1,
    })

    assert outcome.success is True
    assert outcome.remote_series_folder_id == 'series-1'
    assert outcome.remote_folder_id == 'season-1'
    restore = [payload for url, payload in adapter.calls if url.endswith('/restore_share')]
    assert restore == [{'accessToken': 'share-token', 'fileIds': ['incoming'], 'parentId': 'season-1'}]


@pytest.mark.asyncio
async def test_completed_only_root_receives_later_missing_episode_without_new_ongoing_root():
    adapter = LayoutGuangyaAdapter()
    adapter.children['completed-root'] = [
        {'fileId': 'series-1', 'name': '旧剧集 (2025) {tmdbid-1}【完结】', 'resType': 2},
    ]
    adapter.children['series-1'] = [
        {'fileId': 'season-1', 'name': '第一季', 'resType': 2},
    ]
    adapter.children['season-1'] = []

    outcome = await adapter.transfer({
        'share_url': 'https://pan.guangyapan.com/s/share',
        'target_folder_id': 'ongoing-root',
        'ongoing_root_id': 'ongoing-root',
        'completed_root_id': 'completed-root',
        'tmdb_id': 1,
        'media_type': 'tv',
        'auth_token': 'access-token',
        'expected_files': ['S01E02.mkv'],
        'series_folder_name': '旧剧集 (2025) {tmdbid-1}',
        'season_folder_name': 'S01',
        'season': 1,
        'destination_kind': 'ongoing',
        'verify_attempts': 1,
    })

    assert outcome.success is True
    assert outcome.remote_series_folder_id == 'series-1'
    assert outcome.remote_folder_id == 'season-1'
    assert outcome.remote_destination_kind == 'completed'
    assert adapter.children['ongoing-root'] == []
    restore = [payload for url, payload in adapter.calls if url.endswith('/restore_share')]
    assert restore == [{'accessToken': 'share-token', 'fileIds': ['incoming'], 'parentId': 'season-1'}]


@pytest.mark.asyncio
async def test_promotion_only_moves_and_renames_without_restoring_a_share_again():
    adapter = LayoutGuangyaAdapter()
    adapter.children['ongoing-root'] = [
        {'fileId': 'legacy-series', 'name': '旧剧集 (2025) {tmdbid-1}', 'resType': 2},
    ]
    adapter.children['legacy-series'] = [
        {'fileId': 'legacy-s01', 'name': '第一季', 'resType': 2},
    ]
    adapter.children['legacy-s01'] = [
        {'fileId': 'old-episode', 'name': 'E01.mkv', 'resType': 1},
    ]

    outcome = await adapter.transfer({
        'operation': 'promote',
        'target_folder_id': 'completed-root',
        'ongoing_root_id': 'ongoing-root',
        'completed_root_id': 'completed-root',
        'tmdb_id': 1,
        'media_type': 'tv',
        'auth_token': 'access-token',
        'series_folder_name': '旧剧集 (2025) {tmdbid-1}',
        'season_folder_name': 'S01',
        'destination_kind': 'completed',
        'promotion_source_series_folder_id': 'legacy-series',
        'promotion_expected_files_by_season': {'S01': ['E01.mkv']},
    })

    assert outcome.success is True
    assert outcome.verified is True
    assert outcome.remote_series_folder_id == 'legacy-series'
    assert outcome.remote_folder_id == 'legacy-s01'
    assert adapter.children['completed-root'][0]['name'] == '旧剧集 (2025) {tmdbid-1}【完结】'
    assert adapter.children['ongoing-root'] == []
    assert not any(url.endswith('/restore_share') for url, _ in adapter.calls)
    move_index = next(index for index, (url, _payload) in enumerate(adapter.calls) if url.endswith('/file/move_file'))
    rename_index = next(index for index, (url, _payload) in enumerate(adapter.calls) if url.endswith('/file/rename'))
    assert move_index < rename_index
    assert any(
        url.endswith('/file/get_file_list') and payload.get('parentId') == 'legacy-series'
        for url, payload in adapter.calls[move_index + 1:rename_index]
    )


@pytest.mark.asyncio
async def test_incomplete_promotion_readback_never_adds_completed_marker():
    adapter = LayoutGuangyaAdapter()
    adapter.children['ongoing-root'] = [
        {'fileId': 'legacy-series', 'name': '旧剧集 (2025) {tmdbid-1}', 'resType': 2},
    ]

    with pytest.raises(PromotionUnverifiedError):
        await adapter.transfer({
            'operation': 'promote',
            'target_folder_id': 'completed-root',
            'ongoing_root_id': 'ongoing-root',
            'completed_root_id': 'completed-root',
            'tmdb_id': 1,
            'media_type': 'tv',
            'auth_token': 'access-token',
            'series_folder_name': '旧剧集 (2025) {tmdbid-1}',
            'season_folder_name': 'S01',
            'destination_kind': 'completed',
            'promotion_source_series_folder_id': 'legacy-series',
            'promotion_expected_files_by_season': {'S01': ['S01E99.mkv']},
        })

    assert adapter.children['completed-root'][0]['name'] == '旧剧集 (2025) {tmdbid-1}'
    assert not any(url.endswith('/file/rename') for url, _ in adapter.calls)
    assert not any(url.endswith('/restore_share') for url, _ in adapter.calls)


@pytest.mark.asyncio
async def test_read_only_directory_listing_never_calls_restore_or_move():
    adapter = LayoutGuangyaAdapter()
    adapter.children['ongoing-root'] = [
        {'fileId': 'legacy-series', 'name': '旧剧集 {tmdbid-1}', 'resType': 2},
        {'fileId': 'loose-file', 'name': 'loose.mkv', 'resType': 1},
    ]

    directories = await adapter.list_directories(auth_token='access-token', parent_id='ongoing-root')

    assert directories == [{'fileId': 'legacy-series', 'name': '旧剧集 {tmdbid-1}', 'resType': 2}]
    assert not any(url.endswith(('/restore_share', '/file/move_file')) for url, _ in adapter.calls)
