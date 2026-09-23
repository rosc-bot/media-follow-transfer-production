"""Phase 2A: transfer verification hardening.

Covers:
  9. expected_files empty + selected_names has videos -> verify with selected_names
 10. expected and selected both empty -> NO_VIDEO_FILES
 11. share with only subtitles/images -> NO_VIDEO_FILES
 12. readback missing files -> READBACK_UNVERIFIED
 13. readback contains all expected -> verified success
"""

import pytest

from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.errors import NoVideoFilesError, ReadbackVerificationError


class VideoVerificationAdapter(GuangyaAdapter):
    """Scripted share + readback listings for verification-tightening tests."""

    def __init__(self, *, share_files, readback_files, expected=None, write_enabled=True):
        super().__init__(write_enabled=write_enabled)
        self.share_files = share_files
        self.readback_files = readback_files
        self.restore_called = 0
        self.expected = expected

    async def post(self, client, url, payload, headers):
        if url.endswith('/get_share_access_token'):
            return {'code': 0, 'data': {'accessToken': 'share-token'}}
        if url.endswith('/get_share_page_files_list'):
            return {'code': 0, 'data': {'list': self.share_files, 'hasMore': False}}
        if url.endswith('/file/get_file_list'):
            if str(payload.get('parentId') or '') == 'target':
                return {'code': 0, 'data': {'list': self.readback_files, 'hasMore': False}}
            return {'code': 0, 'data': {'list': [], 'hasMore': False}}
        if url.endswith('/restore_share'):
            self.restore_called += 1
            return {'code': 0, 'data': {}}
        raise AssertionError(url)


def _payload(**overrides):
    base = {
        'share_url': 'https://pan.guangyapan.com/s/share',
        'target_folder_id': 'target',
        'auth_token': 'access-token',
        'verify_attempts': 2,
        'verify_interval_seconds': 0,
    }
    base.update(overrides)
    return base


@pytest.mark.asyncio
async def test_expected_empty_uses_selected_names_for_readback_verification():
    adapter = VideoVerificationAdapter(
        share_files=[{'fileId': '1', 'name': 'a.mkv', 'resType': 1}, {'fileId': '2', 'name': 'b.mp4', 'resType': 1}],
        readback_files=[{'name': 'a.mkv', 'resType': 1}, {'name': 'b.mp4', 'resType': 1}],
    )

    outcome = await adapter.transfer(_payload(expected_files=[]))

    assert outcome.success is True
    assert outcome.verified is True
    assert outcome.remote_files == ('a.mkv', 'b.mp4')
    assert adapter.restore_called == 1


@pytest.mark.asyncio
async def test_no_files_at_all_raises_no_video_files():
    adapter = VideoVerificationAdapter(share_files=[], readback_files=[])

    with pytest.raises(NoVideoFilesError):
        await adapter.transfer(_payload(expected_files=[]))


@pytest.mark.asyncio
async def test_share_with_only_subtitles_and_images_raises_no_video_files():
    adapter = VideoVerificationAdapter(
        share_files=[
            {'fileId': '1', 'name': 'sub.srt', 'resType': 1},
            {'fileId': '2', 'name': 'cover.jpg', 'resType': 1},
            {'fileId': '3', 'name': 'info.txt', 'resType': 1},
            {'fileId': '4', 'name': 'movie.nfo', 'resType': 1},
        ],
        readback_files=[],
    )

    with pytest.raises(NoVideoFilesError):
        await adapter.transfer(_payload(expected_files=[]))


@pytest.mark.asyncio
async def test_readback_missing_files_raises_readback_unverified():
    adapter = VideoVerificationAdapter(
        share_files=[{'fileId': '1', 'name': 'a.mkv', 'resType': 1}, {'fileId': '2', 'name': 'b.mkv', 'resType': 1}],
        readback_files=[{'name': 'a.mkv', 'resType': 1}],  # b.mkv missing after restore
    )

    with pytest.raises(ReadbackVerificationError):
        await adapter.transfer(_payload(expected_files=['a.mkv', 'b.mkv']))
    assert adapter.restore_called == 1


@pytest.mark.asyncio
async def test_readback_with_all_expected_files_is_verified_success():
    adapter = VideoVerificationAdapter(
        share_files=[{'fileId': '1', 'name': 'a.mkv', 'resType': 1}],
        readback_files=[{'name': 'a.mkv', 'resType': 1}],
    )

    outcome = await adapter.transfer(_payload(expected_files=['a.mkv']))

    assert outcome.success is True
    assert outcome.verified is True
    assert outcome.remote_files == ('a.mkv',)
    assert adapter.restore_called == 1


@pytest.mark.asyncio
async def test_expected_files_missing_from_share_raises_episode_mismatch():
    adapter = VideoVerificationAdapter(
        share_files=[{'fileId': '1', 'name': 'a.mkv', 'resType': 1}],
        readback_files=[{'name': 'a.mkv', 'resType': 1}],
    )

    from app.transfer.errors import GuangyaTransferError, TransferErrorCategory

    with pytest.raises(GuangyaTransferError) as excinfo:
        await adapter.transfer(_payload(expected_files=['a.mkv', 'missing.mkv']))
    assert excinfo.value.category == TransferErrorCategory.EPISODE_MISMATCH
    assert adapter.restore_called == 0  # never restore when the share cannot satisfy expected files
