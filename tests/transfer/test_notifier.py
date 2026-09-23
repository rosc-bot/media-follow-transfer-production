from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.transfer.notifier import TransferNotifier


@pytest.mark.asyncio
async def test_notifier_success_pushes_to_source_channel():
    notifier = TransferNotifier(
        bot_token='123456:FAKE_TOKEN',
        admin_tg_id=8586984520,
        default_channel_id='-1004387965244',
    )
    mock_resp = httpx.Response(200, json={'ok': True, 'result': {'message_id': 999}}, request=httpx.Request('POST', 'https://example.com'))
    with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_resp
        task_payload = {
            'title': '测试剧集',
            'season': 1,
            'episode_keys': ['S01E01', 'S01E02'],
            'provider': 'guangya',
            'source_channel_id': '-1003808659413',
        }
        transfer_result = {
            'verified': True,
            'remote_files': ['测试剧集.S01E01.mkv', '测试剧集.S01E02.mkv'],
        }
        sent = await notifier.notify_success(
            task_payload=task_payload,
            transfer_result=transfer_result,
        )
        assert sent is True
        assert mock_post.called
        call_args = mock_post.call_args
        assert call_args[0][0] == 'https://api.telegram.org/bot123456:FAKE_TOKEN/sendMessage'
        body = call_args[1]['json']
        assert body['chat_id'] == '@guangyazhauncun'
        assert '测试剧集' in body['text']
        assert 'S01E01-E02（2集）' in body['text']
        assert '测试剧集.S01E01.mkv' in body['text']


@pytest.mark.asyncio
async def test_notifier_failure_pushes_to_admin_bot():
    notifier = TransferNotifier(
        bot_token='123456:FAKE_TOKEN',
        admin_tg_id=8586984520,
        default_channel_id='-1004387965244',
    )
    mock_resp = httpx.Response(200, json={'ok': True, 'result': {'message_id': 1000}}, request=httpx.Request('POST', 'https://example.com'))
    with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_resp
        task_payload = {
            'title': '失败剧集',
            'episode_keys': ['S01E03'],
            'share_url': 'https://pan.guangyapan.com/s/failed',
        }
        sent = await notifier.notify_failure(
            task_payload=task_payload,
            error_message='Token expired or invalid',
            attempts=3,
        )
        assert sent is True
        assert mock_post.called
        call_args = mock_post.call_args
        assert call_args[0][0] == 'https://api.telegram.org/bot123456:FAKE_TOKEN/sendMessage'
        body = call_args[1]['json']
        assert body['chat_id'] == 8586984520
        assert '失败剧集' in body['text']
        assert 'Token expired or invalid' in body['text']
        assert '第 3 次' in body['text']


@pytest.mark.asyncio
async def test_notifier_handles_network_error_gracefully():
    notifier = TransferNotifier(
        bot_token='123456:FAKE_TOKEN',
        admin_tg_id=8586984520,
    )
    with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
        mock_post.side_effect = httpx.ConnectError('connection failed')
        sent = await notifier.notify_failure(
            task_payload={'title': '异常剧集'},
            error_message='something wrong',
        )
        assert sent is False
