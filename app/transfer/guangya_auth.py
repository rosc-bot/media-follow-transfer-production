"""Unified Guangya credential handling.

Single responsibility areas
---------------------------
* :class:`GuangyaAuthContext` — in-memory tokens for one request chain plus a
  strict "refreshed once" guard.
* :class:`GuangyaCredentialProvider` — network layer: exactly one controlled
  refresh against the account endpoint, with explicit status handling
  (200/400/401/403/429/5xx/timeout/malformed JSON). No database dependency.
* :class:`GuangyaCredentialStore` — persistence layer: atomically merges the
  refreshed access token (and an optional rotated refresh token) into
  ``cloud_configs.auth_ref`` inside one transaction, then re-reads and verifies
  the stored JSON is parseable. Never logs token contents.

Both layers are used by the Guangya adapter so list/create/move/rename/restore/
readback all share the exact same 401 -> single refresh -> retry path.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import socket
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.cloud import CloudConfig
from app.transfer.errors import (
    GuangyaTransferError,
    TransferErrorCategory,
)

logger = logging.getLogger(__name__)

ACCOUNT_BASE = 'https://account.guangyapan.com'
REFRESH_PATH = '/v1/auth/token'
CLIENT_ID = 'aMe-8VSlkrbQXpUR'
REFRESH_REQUEST_TIMEOUT = httpx.Timeout(12.0, connect=5.0, read=8.0, write=5.0, pool=2.0)
REFRESH_DNS_TIMEOUT_SECONDS = 4.0
REFRESH_MAX_ATTEMPTS = 2
REFRESH_RETRY_BACKOFF_SECONDS = 0.25
_SENSITIVE_ERROR_RE = re.compile(
    r'(?i)\b(access[_-]?token|refresh[_-]?token|authorization|cookie|auth_ref|password|secret|api[_-]?key)\b(\s*[:=]\s*)[^\s,;]+'
)
_ERROR_QUERY_RE = re.compile(r'(https?://[^\s?#]+)\?[^\s#]+')
_BEARER_ERROR_RE = re.compile(r'(?i)\bBearer\s+[^\s,;]+')


def _safe_exception_message(exc: BaseException) -> str:
    message = str(exc)
    message = _ERROR_QUERY_RE.sub(r'\1?[REDACTED_QUERY]', message)
    message = _SENSITIVE_ERROR_RE.sub(r'\1\2[REDACTED]', message)
    return _BEARER_ERROR_RE.sub('Bearer [REDACTED]', message)[:400]


def _trace_stage(event_name: str) -> str | None:
    lowered = str(event_name).casefold()
    if 'start_tls' in lowered:
        return 'TLS_HANDSHAKE'
    if 'connect_tcp' in lowered:
        return 'TCP_CONNECT'
    if 'send_request_headers' in lowered or 'send_request_body' in lowered:
        return 'REQUEST_WRITE'
    if 'receive_response_headers' in lowered or 'receive_response_body' in lowered:
        return 'RESPONSE_READ'
    return None


def default_device_id() -> str:
    """Stable 32-hex device id for account/API headers.

    Guangya does not bind these headers to the original login device for the
    documented token flows, but the API *requires* a well-formed device header
    set (x-action: 401 on refresh, x-device-* on account calls, did/dt on API
    calls). A deterministic id keeps every worker node on the same identity.
    """
    return hashlib.md5(f'{CLIENT_ID}@guangyapan'.encode()).hexdigest()

#: Video extensions the pipeline may restore; anything else is not a transfer target.
VIDEO_EXTENSIONS = frozenset({
    '.mkv', '.mp4', '.avi', '.mov', '.ts', '.m2ts', '.webm',
    '.wmv', '.flv', '.m4v', '.rmvb', '.rm', '.3gp', '.mpeg', '.mpg',
})


def is_video_filename(name: str) -> bool:
    """Return True when *name* ends with a supported video extension."""
    lowered = str(name or '').strip().lower()
    return any(lowered.endswith(ext) for ext in VIDEO_EXTENSIONS)


def sanitize_token(value: str | None) -> str:
    """Redact a token for logs: never the full value, only a short tail."""
    if not value:
        return '(none)'
    tail = str(value)[-4:]
    return f'gy…{tail}'


def parse_auth_ref(raw: str | None) -> dict[str, Any]:
    """Parse a CloudConfig.auth_ref blob into a dict.

    Accepts JSON blobs (``{"access_token": ..., "refresh_token": ...}``), a bare
    refresh token (``gy.xxx``), or a two-token whitespace string. Unknown raw
    values degrade to an empty dict instead of raising.
    """
    text = (raw or '').strip()
    if not text:
        return {}
    if text.startswith('{'):
        try:
            data = json.loads(text)
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}
    parts = text.split()
    if len(parts) == 1:
        return {'refresh_token': text} if text.startswith('gy.') else {'access_token': text}
    return {'access_token': parts[0], 'refresh_token': parts[1]} if len(parts) >= 2 else {}


def extract_auth_tokens(data: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return (access_token, refresh_token) from a parsed blob (JSON or refreshed payload)."""
    access = (
        data.get('access_token')
        or data.get('token')
        or data.get('accessToken')
    )
    refresh = data.get('refresh_token') or data.get('refreshToken')
    return (str(access) if access else None, str(refresh) if refresh else None)


def build_auth_ref(payload: dict[str, Any]) -> str:
    """Serialize an auth_ref JSON blob without leaking ordering surprises."""
    return json.dumps(payload, ensure_ascii=False, separators=(',', ':'))


@dataclass
class GuangyaAuthContext:
    """One request chain's credential state; refresh is allowed at most once."""

    access_token: str | None = None
    refresh_token: str | None = None
    refreshed_this_chain: bool = False
    extra_fields: dict[str, Any] = field(default_factory=dict)

    def apply_refreshed(self, access: str, refresh: str | None) -> None:
        self.access_token = access
        if refresh:
            self.refresh_token = refresh
        self.refreshed_this_chain = True


def context_from_auth_ref(raw: str | None) -> GuangyaAuthContext:
    """Build a :class:`GuangyaAuthContext` from a stored auth_ref blob."""
    data = parse_auth_ref(raw)
    access, refresh = extract_auth_tokens(data)
    extra = {
        key: value
        for key, value in data.items()
        if key not in {'access_token', 'token', 'accessToken', 'refresh_token', 'refreshToken'}
    }
    return GuangyaAuthContext(access_token=access, refresh_token=refresh, extra_fields=extra)


class GuangyaCredentialProvider:
    """Network layer for the account refresh endpoint (no database dependency)."""

    def __init__(self, client_id: str = CLIENT_ID, device_id: str | None = None) -> None:
        self.client_id = client_id
        self.device_id = device_id or default_device_id()

    def _account_headers(self) -> dict[str, str]:
        """Full browser-equivalent header set required by the Guangya account API.

        Without ``x-action: 401`` the refresh endpoint behaves differently and
        returns unusable credentials; the x-device-* headers are mandatory for
        the whole account surface.
        """
        return {
            'accept': '*/*',
            'content-type': 'application/json',
            'origin': 'https://www.guangyapan.com',
            'referer': 'https://www.guangyapan.com/',
            'user-agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36'
            ),
            'x-client-id': self.client_id,
            'x-client-version': '0.0.1',
            'x-device-id': self.device_id,
            'x-device-model': 'chrome%2F147.0.0.0',
            'x-device-name': 'PC-Chrome',
            'x-device-sign': f'wdi10.{self.device_id}0000000000000000',
            'x-net-work-type': 'NONE',
            'x-os-version': 'MacIntel',
            'x-platform-version': '1',
            'x-protocol-version': '301',
            'x-provider-name': 'NONE',
            'x-sdk-version': '9.0.2',
            'x-action': '401',
        }

    def _api_headers(self, access_token: str | None = None) -> dict[str, str]:
        headers = {
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json',
            'origin': 'https://www.guangyapan.com',
            'referer': 'https://www.guangyapan.com/',
            'did': self.device_id,
            'dt': '4',
        }
        if access_token:
            headers['authorization'] = f'Bearer {access_token}'
        return headers

    async def _request_refresh(
        self,
        client: httpx.AsyncClient,
        refresh_token: str,
    ) -> httpx.Response:
        """Perform a bounded account refresh and retain the failing transport stage."""
        url = f'{ACCOUNT_BASE}{REFRESH_PATH}'
        response: httpx.Response | None = None
        for attempt in range(1, REFRESH_MAX_ATTEMPTS + 1):
            started = time.monotonic()
            stage = 'DNS_RESOLUTION'
            trace_state = {'stage': ''}

            async def trace(event_name: str, _info: dict[str, Any], _state=trace_state) -> None:
                if str(event_name).endswith('.started'):
                    traced_stage = _trace_stage(event_name)
                    if traced_stage:
                        _state['stage'] = traced_stage

            try:
                if isinstance(client, httpx.AsyncClient):
                    host = urlsplit(url).hostname
                    if host:
                        await asyncio.wait_for(
                            asyncio.get_running_loop().getaddrinfo(host, 443, type=socket.SOCK_STREAM),
                            timeout=REFRESH_DNS_TIMEOUT_SECONDS,
                        )
                stage = 'TCP_CONNECT'
                response = await client.post(
                    url,
                    json={
                        'client_id': self.client_id,
                        'grant_type': 'refresh_token',
                        'refresh_token': refresh_token,
                    },
                    headers=self._account_headers(),
                    timeout=REFRESH_REQUEST_TIMEOUT,
                    extensions={'trace': trace},
                )
                break
            except (httpx.TimeoutException, TimeoutError, httpx.NetworkError, OSError) as exc:
                if isinstance(exc, httpx.PoolTimeout):
                    stage = 'CONNECTION_POOL_WAIT'
                elif isinstance(exc, httpx.WriteTimeout):
                    stage = 'REQUEST_WRITE'
                elif isinstance(exc, httpx.ReadTimeout):
                    stage = 'RESPONSE_READ'
                elif trace_state['stage']:
                    stage = trace_state['stage']
                elif isinstance(exc, TimeoutError) and stage == 'DNS_RESOLUTION':
                    stage = 'DNS_TIMEOUT'
                elif isinstance(exc, (httpx.ConnectTimeout, httpx.ConnectError)):
                    stage = 'TCP_CONNECT'
                elapsed_ms = round((time.monotonic() - started) * 1000)
                exception_message = _safe_exception_message(exc)
                category = (
                    TransferErrorCategory.NETWORK_TIMEOUT
                    if isinstance(exc, (httpx.TimeoutException, TimeoutError))
                    else TransferErrorCategory.NETWORK_ERROR
                )
                logger.warning(
                    'Guangya request failed endpoint_type=account stage=%s elapsed_ms=%s exception_type=%s exception_message=%s attempt=%s/%s',
                    stage,
                    elapsed_ms,
                    type(exc).__name__,
                    exception_message,
                    attempt,
                    REFRESH_MAX_ATTEMPTS,
                )
                if attempt < REFRESH_MAX_ATTEMPTS:
                    await asyncio.sleep(REFRESH_RETRY_BACKOFF_SECONDS)
                    continue
                raise GuangyaTransferError(
                    category,
                    f'guangya refresh failed endpoint_type=account stage={stage} elapsed_ms={elapsed_ms} '
                    f'exception_type={type(exc).__name__} exception_message={exception_message}',
                ) from exc
        if response is None:
            raise GuangyaTransferError(
                TransferErrorCategory.NETWORK_ERROR,
                'guangya refresh failed endpoint_type=account stage=UNKNOWN response_missing=true',
            )
        return response

    async def refresh_access(self, client: httpx.AsyncClient, refresh_token: str) -> tuple[str, str | None]:
        """Exchange *refresh_token* for a fresh access token.

        Returns ``(new_access, new_refresh_or_None)``. The refresh token the API
        returns (if any) is rotated; otherwise the caller must keep the old one.

        Raises a categorized :class:`GuangyaTransferError` for every non-200 path:
        AUTH_INVALID (400/401/403 or malformed body), RATE_LIMITED (429),
        REMOTE_5XX (5xx), NETWORK_TIMEOUT / NETWORK_ERROR for transport faults.
        """
        response = await self._request_refresh(client, refresh_token)

        status = response.status_code
        if status == 200:
            try:
                data = response.json()
            except (json.JSONDecodeError, ValueError) as exc:
                raise GuangyaTransferError(
                    TransferErrorCategory.AUTH_INVALID, 'guangya refresh response is not valid JSON'
                ) from exc
            if not isinstance(data, dict):
                raise GuangyaTransferError(
                    TransferErrorCategory.AUTH_INVALID, 'guangya refresh response is not an object'
                )
            inner = data.get('data')
            payload = inner if isinstance(inner, dict) else data
            access = (
                payload.get('access_token')
                or payload.get('accessToken')
                or data.get('access_token')
                or data.get('accessToken')
            )
            if not access:
                raise GuangyaTransferError(
                    TransferErrorCategory.AUTH_INVALID, 'guangya refresh response missing access token'
                )
            refresh = (
                payload.get('refresh_token')
                or payload.get('refreshToken')
                or data.get('refresh_token')
                or data.get('refreshToken')
            )
            return str(access), (str(refresh) if refresh else None)

        if status == 429:
            raise GuangyaTransferError(TransferErrorCategory.RATE_LIMITED, 'guangya refresh rate limited (429)')
        if status >= 500:
            raise GuangyaTransferError(TransferErrorCategory.REMOTE_5XX, f'guangya refresh remote error ({status})')
        if status in (400, 401, 403):
            raise GuangyaTransferError(TransferErrorCategory.AUTH_INVALID, f'guangya refresh rejected ({status})')
        raise GuangyaTransferError(TransferErrorCategory.UNKNOWN, f'guangya refresh unexpected status ({status})')


class GuangyaCredentialStore:
    """Persistence layer: merge refreshed credentials into auth_ref in one transaction."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def persist_refresh(self, provider: str, refreshed: dict[str, str]) -> bool:
        """Atomically merge *refreshed* (``access_token`` plus optional ``refresh_token``)
        into ``cloud_configs.auth_ref`` for *provider*.

        The old refresh token is kept unless the API provided a new one. Unrelated
        auth_ref fields survive. The stored blob is re-read and parsed to prove the
        update is not a half-written JSON fragment.
        """
        async with self.session_factory() as db, db.begin():
            row = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
            if row is None:
                raise GuangyaTransferError(
                    TransferErrorCategory.AUTH_INVALID, f'cloud provider {provider!r} is not configured'
                )
            merged = parse_auth_ref(row.auth_ref)
            access = refreshed.get('access_token')
            if not access:
                raise GuangyaTransferError(TransferErrorCategory.AUTH_INVALID, 'cannot persist an empty access token')
            merged['access_token'] = access
            if refreshed.get('refresh_token'):
                merged['refresh_token'] = refreshed['refresh_token']
            serialized = build_auth_ref(merged)
            await db.execute(
                update(CloudConfig)
                .where(CloudConfig.name == provider)
                .values(auth_ref=serialized)
            )
            stored = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider))
            if stored is None:
                raise GuangyaTransferError(TransferErrorCategory.AUTH_INVALID, 'cloud config disappeared during refresh persist')
            readback = parse_auth_ref(stored.auth_ref)
            if readback.get('access_token') != access:
                raise GuangyaTransferError(
                    TransferErrorCategory.AUTH_INVALID, 'refresh persist readback did not match stored access token'
                )
        logger.info('Guangya credential refreshed successfully')
        return True
