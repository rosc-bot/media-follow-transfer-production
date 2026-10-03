"""Recorded provider business responses; all IO is mocked and writes are isolated."""
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from app.follow.follow_worker import run_follow_cycle
from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.errors import GuangyaAuthExpiredError, GuangyaTransferError, TransferErrorCategory, classify_error
from app.transfer.guangya_auth import GuangyaAuthContext
from app.transfer.queue_service import TransferQueueService
from app.transfer.queue_worker import TransferQueueWorker

URL = 'https://api.guangyapan.com/nd.bizuserres.s/v1/restore_share'

@pytest.mark.parametrize('body', [{'code':157}, {'code':117}, {'code':157,'msg':'success'},
                                  {'unexpected':1}, {'code':112,'msg':None}])
def test_business_failure_never_counts_as_success(body):
    assert GuangyaAdapter.response_ok(body) is False

@pytest.mark.parametrize('body', [{'code':0}, {'code':'200'}, {'msg':'success','data':{'list':[]}},
                                  {'data':{'list':[]}}, {'code':0,'msg':None}])
def test_existing_success_shapes_still_work(body):
    assert GuangyaAdapter.response_ok(body) is True

@pytest.mark.asyncio
async def test_recorded_space_error_reaches_nonretryable_queue_policy():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={'code':157,'msg':'空间不足'}))) as client:
        with pytest.raises(GuangyaTransferError) as caught:
            await GuangyaAdapter(write_enabled=False).post(client, URL, {}, {})
    category = classify_error(caught.value)
    assert category == TransferErrorCategory.INSUFFICIENT_SPACE
    task = SimpleNamespace(status='RUNNING', error_message=None, attempt_count=1, max_attempts=8, locked_at=None, locked_by=None)
    db = SimpleNamespace(flush=AsyncMock())
    await TransferQueueService.mark_failed(db, task, str(caught.value), category=category)
    assert task.status == 'FAILED'
    assert 'INSUFFICIENT_SPACE' in task.error_message

@pytest.mark.asyncio
@pytest.mark.parametrize('repeated', [False, True])
async def test_http_200_invalid_token_refreshes_once_and_persists(repeated):
    calls = []
    def respond(request):
        calls.append(request.headers.get('authorization'))
        if len(calls) == 1 or repeated:
            return httpx.Response(200, json={'code':117,'msg':'无效token'})
        return httpx.Response(200, json={'code':0,'data':{'list':[]}})
    provider = SimpleNamespace(_api_headers=lambda token: {'authorization': f'Bearer {token}'},
                              refresh_access=AsyncMock(return_value=('new-access',None)))
    store = SimpleNamespace(persist_refresh=AsyncMock())
    adapter = GuangyaAdapter(write_enabled=False, credential_provider=provider, credential_store=store)
    ctx = GuangyaAuthContext(access_token='old-access', refresh_token='test-refresh')
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        if repeated:
            with pytest.raises(GuangyaAuthExpiredError):
                await adapter._authorized_post(client, URL, {}, ctx)
        else:
            result = await adapter._authorized_post(client, URL, {}, ctx)
            assert result['code'] == 0
    assert calls == ['Bearer old-access', 'Bearer new-access']
    provider.refresh_access.assert_awaited_once()
    store.persist_refresh.assert_awaited_once_with('guangya', {'access_token':'new-access'})

@pytest.mark.asyncio
async def test_invalid_token_without_refresh_is_terminal():
    ctx = GuangyaAuthContext(access_token='old-access', refresh_token=None)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={'code':117,'msg':'无效token'}))) as client:
        with pytest.raises(GuangyaAuthExpiredError):
            await GuangyaAdapter(write_enabled=False)._authorized_post(client, URL, {}, ctx)

@pytest.mark.asyncio
async def test_parameter_error_does_not_refresh_credentials():
    provider = SimpleNamespace(_api_headers=lambda token: {}, refresh_access=AsyncMock())
    adapter = GuangyaAdapter(write_enabled=False, credential_provider=provider)
    ctx = GuangyaAuthContext(access_token='old-access', refresh_token='test-refresh')
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={'code':112,'msg':'参数错误'}))) as client:
        with pytest.raises(RuntimeError, match='112'):
            await adapter._authorized_post(client, URL, {}, ctx)
    provider.refresh_access.assert_not_awaited()

class DummySession:
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass

@pytest.mark.asyncio
async def test_paused_transfer_keeps_zero_claim_without_info_log_spam(monkeypatch, caplog):
    monkeypatch.setattr('app.transfer.queue_worker.BotSettingsService.is_transfer_paused', AsyncMock(return_value=True))
    claim = AsyncMock()
    monkeypatch.setattr('app.transfer.queue_worker.TransferQueueService.claim_next', claim)
    worker = TransferQueueWorker(session_factory=DummySession)
    with caplog.at_level(logging.INFO):
        for _ in range(3):
            assert await worker._claim_payload() is None
    claim.assert_not_awaited()
    assert not any('transfer pause is enabled' in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_paused_follow_keeps_zero_io_without_info_log_spam(monkeypatch, caplog):
    monkeypatch.setattr('app.follow.follow_worker.BotSettingsService.is_follow_paused', AsyncMock(return_value=True))
    with caplog.at_level(logging.INFO):
        for _ in range(3):
            assert await run_follow_cycle(None, calendar=None, scout=None) == {'synced_watchlists':0,'scout_jobs':0}
    assert not any('follow pause is enabled' in r.message for r in caplog.records)
