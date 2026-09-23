"""Phase 2A: unified Guangya credential flow.

Covers:
  1. valid access -> direct success, no refresh
  2. expired access -> 401 -> refresh -> retry success
  3. 401 -> refresh failure -> AUTH_EXPIRED
  4. 401 -> refresh success -> retry still 401 -> AUTH_EXPIRED
  5. refresh happens at most once per request chain
  6. refreshed access persisted safely (other auth_ref fields preserved)
  7. refresh token is never dropped on persistence
  8. logs never contain tokens
 16. auth errors never enter the ordinary exponential retry loop
"""

import json

import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.cloud import CloudConfig
from app.models.transfer import TransferQueueTask
from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.errors import (
    GuangyaAuthExpiredError,
    GuangyaTransferError,
    TransferErrorCategory,
)
from app.transfer.guangya_auth import (
    GuangyaCredentialProvider,
    GuangyaCredentialStore,
    build_auth_ref,
    sanitize_token,
)
from app.transfer.queue_service import TransferQueueService
from app.transfer.status import TransferStatus


def _unauthorized() -> httpx.HTTPStatusError:
    request = httpx.Request('POST', 'https://api.guangyapan.com/nd.bizuserres.s/v1/file/get_file_list')
    return httpx.HTTPStatusError('401 Unauthorized', request=request, response=httpx.Response(401, request=request))


class GuangyaAdapterStub(GuangyaAdapter):
    """Rebinds post(); the auth wrapper treats post() as the transport."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.post_calls = 0
        self.headers_seen: list[str | None] = []

    async def post(self, client, url, payload, headers):
        raise NotImplementedError


class RecordingProvider(GuangyaCredentialProvider):
    """Records refresh calls and returns fresh credentials (or raises)."""

    def __init__(self, fail_with=None, result=('new-access', None)):
        super().__init__()
        self.calls = 0
        self.fail_with = fail_with
        self.result = result
        self.last_refresh_token = None

    async def refresh_access(self, client, refresh_token):
        self.calls += 1
        self.last_refresh_token = refresh_token
        if self.fail_with is not None:
            raise self.fail_with
        return self.result


AUTH_REF = json.dumps({'access_token': 'expired-access', 'refresh_token': 'refresh-old'})


class FirstCallUnauthorizedAdapter(GuangyaAdapterStub):
    async def post(self, client, url, payload, headers):
        self.headers_seen.append(headers.get('authorization'))
        self.post_calls += 1
        if self.post_calls == 1:
            raise _unauthorized()
        return {'code': 0, 'data': {'list': [{'fileId': 'f1', 'name': 'ongoing', 'resType': 2}], 'hasMore': False}}


@pytest.mark.asyncio
async def test_valid_access_is_used_without_refresh():
    provider = RecordingProvider()

    class OkAdapter(GuangyaAdapterStub):
        async def post(self, client, url, payload, headers):
            self.post_calls += 1
            assert headers.get('authorization') == 'Bearer valid-access'
            return {'code': 0, 'data': {'list': [{'fileId': 'f1', 'name': 'ongoing', 'resType': 2}], 'hasMore': False}}

    adapter = OkAdapter(credential_provider=provider)
    folders = await adapter.list_directories(
        auth_token=json.dumps({'access_token': 'valid-access', 'refresh_token': 'refresh-old'}),
        parent_id='root',
    )
    assert provider.calls == 0
    assert [folder['name'] for folder in folders] == ['ongoing']


@pytest.mark.asyncio
async def test_expired_access_401_refresh_then_retry_success():
    provider = RecordingProvider()
    adapter = FirstCallUnauthorizedAdapter(credential_provider=provider)

    folders = await adapter.list_directories(auth_token=AUTH_REF, parent_id='root')

    assert provider.calls == 1
    assert provider.last_refresh_token == 'refresh-old'
    assert adapter.post_calls == 2
    assert adapter.headers_seen == ['Bearer expired-access', 'Bearer new-access']
    assert [folder['name'] for folder in folders] == ['ongoing']


@pytest.mark.asyncio
async def test_refresh_failure_raises_auth_expired():
    provider = RecordingProvider(
        fail_with=GuangyaTransferError(TransferErrorCategory.AUTH_INVALID, 'refresh rejected')
    )
    adapter = FirstCallUnauthorizedAdapter(credential_provider=provider)

    with pytest.raises(GuangyaAuthExpiredError):
        await adapter.list_directories(auth_token=AUTH_REF, parent_id='root')
    assert provider.calls == 1


@pytest.mark.asyncio
async def test_refresh_success_but_retry_still_401_raises_auth_expired():
    provider = RecordingProvider()

    class AlwaysUnauthorized(GuangyaAdapterStub):
        async def post(self, client, url, payload, headers):
            self.headers_seen.append(headers.get('authorization'))
            self.post_calls += 1
            raise _unauthorized()

    adapter = AlwaysUnauthorized(credential_provider=provider)
    with pytest.raises(GuangyaAuthExpiredError):
        await adapter.list_directories(auth_token=AUTH_REF, parent_id='root')
    assert provider.calls == 1
    assert adapter.post_calls == 2


@pytest.mark.asyncio
async def test_refresh_happens_at_most_once_per_request_chain():
    provider = RecordingProvider()

    class TripleUnauthorized(GuangyaAdapterStub):
        async def post(self, client, url, payload, headers):
            self.post_calls += 1
            raise _unauthorized()

    adapter = TripleUnauthorized(credential_provider=provider)
    with pytest.raises(GuangyaAuthExpiredError):
        await adapter.list_directories(auth_token=AUTH_REF, parent_id='root')
    assert provider.calls == 1
    assert adapter.post_calls == 2  # original request + one retry, never a third


@pytest.mark.asyncio
async def test_refreshed_access_is_persisted_without_losing_other_fields(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/creds.db')
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        db.add(CloudConfig(
            name='guangya',
            auth_ref=json.dumps({'access_token': 'expired-access', 'refresh_token': 'refresh-old', 'extra': 'keep-me'}),
            enabled=True,
        ))

    store = GuangyaCredentialStore(sessions)
    assert await store.persist_refresh('guangya', {'access_token': 'new-access'}) is True

    async with sessions() as db:
        row = await db.get(CloudConfig, 1)
        merged = json.loads(row.auth_ref)
        assert merged['access_token'] == 'new-access'
        assert merged['refresh_token'] == 'refresh-old'  # never dropped
        assert merged['extra'] == 'keep-me'  # unrelated fields preserved


@pytest.mark.asyncio
async def test_persist_atomically_replaces_both_tokens_when_api_returns_new_refresh(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/creds2.db')
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        db.add(CloudConfig(name='guangya', auth_ref=AUTH_REF, enabled=True))

    store = GuangyaCredentialStore(sessions)
    assert await store.persist_refresh('guangya', {'access_token': 'new-access', 'refresh_token': 'rotated-refresh'}) is True

    async with sessions() as db:
        row = await db.get(CloudConfig, 1)
        merged = json.loads(row.auth_ref)
        assert merged['access_token'] == 'new-access'
        assert merged['refresh_token'] == 'rotated-refresh'


class FakeRefreshServer:
    """Acts as the account endpoint returning new tokens without touching the network."""

    async def post(self, url, json, headers):
        return httpx.Response(200, json={
            'access_token': 'super-secret-new-access',
            'refresh_token': 'super-secret-new-refresh',
            'data': {'access_token': 'super-secret-new-access'},
        })


@pytest.mark.asyncio
async def test_logs_never_contain_tokens(tmp_path, caplog):
    import logging

    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/creds3.db')
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db, db.begin():
        db.add(CloudConfig(name='guangya', auth_ref=AUTH_REF, enabled=True))

    with caplog.at_level(logging.INFO):
        provider = GuangyaCredentialProvider()
        access, refresh = await provider.refresh_access(FakeRefreshServer(), 'refresh-old')
        assert access == 'super-secret-new-access'
        assert refresh == 'super-secret-new-refresh'

        store = GuangyaCredentialStore(sessions)
        await store.persist_refresh('guangya', {'access_token': access, 'refresh_token': refresh})

    combined = caplog.text
    assert 'super-secret-new-access' not in combined
    assert 'super-secret-new-refresh' not in combined
    assert 'refresh-old' not in combined
    assert 'Guangya credential refreshed successfully' in combined


class HeaderRecordingRefreshServer(FakeRefreshServer):
    def __init__(self):
        self.captured_headers = None

    async def post(self, url, json, headers):
        self.captured_headers = dict(headers)
        return await super().post(url, json, headers)


@pytest.mark.asyncio
async def test_refresh_uses_official_account_headers():
    """Root-cause regression: without x-action: 401 + device headers the refresh
    endpoint returns unusable credentials (verified against guangyaclient)."""
    server = HeaderRecordingRefreshServer()
    provider = GuangyaCredentialProvider()
    access, refresh = await provider.refresh_access(server, 'refresh-old')

    assert access == 'super-secret-new-access'
    assert refresh == 'super-secret-new-refresh'
    headers = server.captured_headers
    assert headers.get('x-action') == '401'
    assert headers.get('x-device-id')
    assert headers.get('x-device-sign', '').startswith('wdi10.')
    assert headers.get('x-client-id') == 'aMe-8VSlkrbQXpUR'


def test_api_headers_include_device_signature():
    provider = GuangyaCredentialProvider()
    api = provider._api_headers('some-token')
    assert api['did'] == provider.device_id
    assert api['dt'] == '4'
    assert api['authorization'] == 'Bearer some-token'


def _business_112() -> httpx.HTTPStatusError:
    """HTTP 200 + business code 112 — Guangya's REAL "参数错误" signal for a
    malformed request (missing pagination params), NOT an auth rejection."""
    request = httpx.Request('POST', 'https://api.guangyapan.com/nd.bizuserres.s/v1/file/get_file_list')
    response = httpx.Response(200, request=request, json={'code': 112, 'msg': '参数错误'})
    return httpx.HTTPStatusError('guangya API rejected request: 112', request=request, response=response)


class PaginationRecordingAdapter(GuangyaAdapterStub):
    async def post(self, client, url, payload, headers):
        self.post_calls += 1
        self.last_payload = dict(payload)
        return {'code': 0, 'data': {'list': [{'fileId': 'f1', 'name': 'ongoing', 'resType': 2}], 'hasMore': False}}


@pytest.mark.asyncio
async def test_list_request_includes_full_pagination_params():
    """Root-cause regression: get_file_list without page/pageSize/orderBy/
    sortType is answered with 200 + code 112 (参数错误). The adapter must always
    send the complete pagination payload (live-verified 2026-09-21)."""
    provider = RecordingProvider()
    adapter = PaginationRecordingAdapter(credential_provider=provider)

    await adapter.list_directories(
        auth_token=json.dumps({'access_token': 'valid-access', 'refresh_token': 'refresh-old'}),
        parent_id='root',
    )

    payload = adapter.last_payload
    assert payload['parentId'] == 'root'
    assert payload['page'] == 0
    assert payload['pageSize'] == GuangyaAdapter.LIST_PAGE_SIZE
    assert payload['orderBy'] == 3
    assert payload['sortType'] == 1
    assert provider.calls == 0  # no auth involved


@pytest.mark.asyncio
async def test_business_code_112_is_not_treated_as_auth_rejection():
    """Regression guard: code 112 means the request parameters were wrong, not
    that the bearer is invalid. A 112 response must NOT trigger a credential
    refresh, and repeated 112s must NOT be surfaced as GuangyaAuthExpiredError."""
    provider = RecordingProvider()

    class AlwaysBusiness112(GuangyaAdapterStub):
        async def post(self, client, url, payload, headers):
            self.post_calls += 1
            raise _business_112()

    adapter = AlwaysBusiness112(credential_provider=provider)
    with pytest.raises(httpx.HTTPStatusError, match='guangya API rejected request'):
        await adapter.list_directories(auth_token=AUTH_REF, parent_id='root')
    assert provider.calls == 0  # 112 is a request error, never a refresh trigger
    assert adapter.post_calls == 1


def test_sanitize_token_redacts_secrets():
    assert 'secret' not in sanitize_token('gy.secret-token-value')
    assert sanitize_token(None) == '(none)'
    assert len(sanitize_token('gy.secret-token-value')) <= 8


def test_build_auth_ref_round_trips_extra_fields():
    payload = {'access_token': 'a', 'refresh_token': 'b', 'folder': 'custom'}
    parsed = json.loads(build_auth_ref(payload))
    assert parsed == payload


# --- queue error classification (item 16) ---


@pytest.mark.asyncio
async def test_auth_expired_never_enters_exponential_retry(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/retry.db')
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        auth_task = await TransferQueueService.enqueue(db, resource_id=1, provider='guangya', payload={})
        auth_task.attempt_count = 1
        auth_task.max_retries = 3
        net_task = await TransferQueueService.enqueue(db, resource_id=2, provider='guangya', payload={})
        net_task.attempt_count = 1
        net_task.max_retries = 3
        await TransferQueueService.mark_failed(db, auth_task, 'token expired', category=TransferErrorCategory.AUTH_EXPIRED)
        await TransferQueueService.mark_failed(db, net_task, 'connection reset', category=TransferErrorCategory.NETWORK_ERROR)

    async with sessions() as db, db.begin():
        from sqlalchemy import select

        auth_task = (await db.scalars(select(TransferQueueTask).where(TransferQueueTask.resource_id == 1))).one()
        net_task = (await db.scalars(select(TransferQueueTask).where(TransferQueueTask.resource_id == 2))).one()
        assert auth_task.status == TransferStatus.FAILED  # never scheduled for retry
        assert auth_task.error_message.startswith('[AUTH_EXPIRED]')
        # FAILED tasks are excluded by claim_next (status filter), so a stale
        # next_run_at can never bring it back into the ordinary retry loop.
        assert net_task.status == TransferStatus.RETRY_WAIT  # network errors still retry
        assert net_task.next_run_at is not None
    await engine.dispose()
