from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from app.transfer.notifier import TransferNotifier


def test_autouse_guard_keeps_notification_logic_real():
    """Safety must intercept network I/O, not replace the subject under test."""
    for name in (
        'notify_success_result', 'notify_failure_result', 'notify_success',
        'notify_failure', '_send_telegram', '_send_telegram_result', '_send_telegram_photo_result',
    ):
        method = getattr(TransferNotifier, name)
        assert method.__module__ == 'app.transfer.notifier', name
        assert method.__name__ == name


@pytest.mark.asyncio
async def test_autouse_guard_blocks_async_telegram_before_network(monkeypatch):
    import httpcore

    network = AsyncMock(side_effect=AssertionError('network must not be reached'))
    monkeypatch.setattr(httpcore.AsyncConnectionPool, 'handle_async_request', network)
    async with httpx.AsyncClient(trust_env=False) as client:
        with pytest.raises(httpx.ConnectError, match='Telegram network access is disabled in tests'):
            await client.post('https://api.telegram.org/botFAKE/sendMessage', json={'chat_id': 1})
    network.assert_not_awaited()


def test_autouse_guard_blocks_sync_telegram_before_network(monkeypatch):
    import httpcore

    network = Mock(side_effect=AssertionError('network must not be reached'))
    monkeypatch.setattr(httpcore.ConnectionPool, 'handle_request', network)
    with httpx.Client(trust_env=False) as client:
        with pytest.raises(httpx.ConnectError, match='Telegram network access is disabled in tests'):
            client.post('https://api.telegram.org/botFAKE/sendMessage', json={'chat_id': 1})
    network.assert_not_called()


@pytest.mark.asyncio
async def test_unmocked_notifier_reports_guarded_send_failure(monkeypatch):
    import httpcore

    network = AsyncMock(side_effect=AssertionError('network must not be reached'))
    monkeypatch.setattr(httpcore.AsyncConnectionPool, 'handle_async_request', network)
    notifier = TransferNotifier(bot_token='123456:FAKE_TOKEN', admin_tg_id=8586984520)
    result = await notifier.notify_failure_result(
        task_payload={'title': '禁止真实发送'}, error_message='test failure',
    )
    # HTTP errors are intentionally normalized to the stable public result codes.
    assert result.status == 'NOTIFICATION_FAILED'
    assert result.sent is False
    assert result.target_chat_id == 8586984520
    assert result.target_source == 'admin_tg_id'
    assert result.error == 'NETWORK'
    network.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('with_poster', [False, True])
async def test_publication_failure_does_not_log_success_or_replace_primary_result(with_poster, caplog):
    import logging

    notifier = TransferNotifier(
        bot_token='123456:FAKE_TOKEN',
        success_chat='@guangyazhauncun',
        publish_chat='@guangyaziyuanfenxiang',
    )
    primary_response = httpx.Response(
        200, json={'ok': True, 'result': {'message_id': 88}},
        request=httpx.Request('POST', 'https://example.invalid'),
    )
    failure_response = httpx.Response(
        403, json={'ok': False, 'description': 'Forbidden: bot was blocked'},
        request=httpx.Request('POST', 'https://example.invalid'),
    )
    payload = {'title': '公开发布失败', 'episode_keys': ['S01E01']}
    if with_poster:
        payload['poster_url'] = 'https://image.tmdb.org/t/p/w500/poster.jpg'
    responses = [primary_response, failure_response]
    if with_poster:
        responses.append(failure_response)
    with caplog.at_level(logging.INFO, logger='app.transfer.notifier'), patch(
        'httpx.AsyncClient.post', new_callable=AsyncMock, side_effect=responses,
    ) as post:
        result = await notifier.notify_success_result(
            task_payload=payload, transfer_result={'verified': True},
        )
    assert result.status == 'SENT'
    assert result.sent is True
    assert result.telegram_message_id == 88
    assert result.target_chat_id == '@guangyazhauncun'
    assert post.await_count == len(responses)
    assert [call.kwargs['json']['chat_id'] for call in post.await_args_list] == [
        '@guangyazhauncun', *(['@guangyaziyuanfenxiang'] * (len(responses) - 1)),
    ]
    assert 'Successfully published resource card' not in caplog.text
    assert 'Resource card publication failed' in caplog.text
    assert 'BOT_FORBIDDEN' in caplog.text


@pytest.mark.asyncio
async def test_notifier_success_pushes_to_source_channel():
    notifier = TransferNotifier(
        bot_token='123456:FAKE_TOKEN',
        admin_tg_id=8586984520,
        default_channel_id='-1004387965244',
        success_chat='@guangyazhauncun',
        publish_chat='@guangyaziyuanfenxiang',
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
        # Dual-channel publication is separate from the primary success result.
        assert mock_post.await_count == 2
        assert [call.kwargs['json']['chat_id'] for call in mock_post.await_args_list] == [
            '@guangyazhauncun', '@guangyaziyuanfenxiang',
        ]
        call_args = mock_post.await_args_list[0]
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
