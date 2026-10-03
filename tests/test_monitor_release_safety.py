"""No real Telegram sessions: verify reconnect replay and client cleanup."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.monitor import telegram_gateway as module


@pytest.mark.asyncio
async def test_gateway_uses_single_client_with_reconnect_catchup(monkeypatch, tmp_path):
    gateway = module.TelegramGateway(
        session_path=str(tmp_path/'unit.session'), api_id=1, api_hash='unit-api-hash',
        summary_db_path=str(tmp_path/'summary.db'), resource_db_path=str(tmp_path/'resources.db'),
    )
    monkeypatch.setattr(gateway, 'load_channel_settings', AsyncMock(return_value={}))
    for name in ('summary_worker','kb_worker','resource_worker','outbox_retry_loop'):
        monkeypatch.setattr(module,name,AsyncMock())
    client = MagicMock()
    client.on.side_effect=lambda event: lambda handler: handler
    client.start=AsyncMock()
    client.get_me=AsyncMock(return_value=SimpleNamespace(first_name='unit',username='unit'))
    client.run_until_disconnected=AsyncMock()
    client.is_connected.return_value=True
    client.disconnect=AsyncMock()
    factory=MagicMock(return_value=client)
    monkeypatch.setattr('telethon.TelegramClient',factory)
    try:
        await gateway.start()
        assert factory.call_count == 1
        assert factory.call_args.kwargs.get('catch_up') is True
    finally:
        await gateway.stop()


@pytest.mark.asyncio
async def test_gateway_transport_failure_closes_original_client_before_retry(monkeypatch):
    gateway=SimpleNamespace(start=AsyncMock(side_effect=RuntimeError('unit transport failure')),stop=AsyncMock())
    monkeypatch.setattr(module,'TelegramGateway',MagicMock(return_value=gateway))
    monkeypatch.setattr(module,'get_settings',lambda:SimpleNamespace(telegram_api_id=1,telegram_api_hash='unit',tg_session_path='unused',summary_db_path='unused',resource_db_path='unused'))
    with pytest.raises(RuntimeError,match='unit transport failure'):
        await module.run_gateway()
    gateway.stop.assert_awaited_once()
