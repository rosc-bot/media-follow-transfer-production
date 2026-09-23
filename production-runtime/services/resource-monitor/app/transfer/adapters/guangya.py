from __future__ import annotations

import asyncio
import json
from typing import ClassVar
from urllib.parse import parse_qs, urlparse

import httpx

from app.core.exceptions import TransferNotAllowed
from app.transfer.adapters import BaseAdapter
from app.transfer.errors import (
    GuangyaAuthExpiredError,
    GuangyaTransferError,
    NoVideoFilesError,
    ReadbackVerificationError,
    TransferErrorCategory,
)
from app.transfer.guangya_auth import (
    GuangyaAuthContext,
    GuangyaCredentialProvider,
    GuangyaCredentialStore,
    context_from_auth_ref,
    is_video_filename,
)
from app.transfer.status import TransferOutcome


class GuangyaAdapter(BaseAdapter):
    """Isolated Guangya adapter: unified credential refresh, paginated selection,
    async restore, verified directory readback (expected_files can never be empty)."""

    provider = 'guangya'
    api_base = 'https://api.guangyapan.com'
    account_base = 'https://account.guangyapan.com'
    client_id = 'aMe-8VSlkrbQXpUR'

    def __init__(
        self,
        *,
        write_enabled: bool | None = None,
        credential_provider: GuangyaCredentialProvider | None = None,
        credential_store: GuangyaCredentialStore | None = None,
    ) -> None:
        super().__init__(write_enabled=write_enabled)
        self.credential_provider = credential_provider or GuangyaCredentialProvider(self.client_id)
        self.credential_store = credential_store

    @staticmethod
    def parse_auth_tokens(raw: str) -> tuple[str | None, str | None]:
        ctx = context_from_auth_ref(raw)
        return ctx.access_token, ctx.refresh_token

    @staticmethod
    def share_parts(url: str) -> tuple[str, str]:
        parsed = urlparse(url if '://' in url else f'https://{url}')
        share_id = parsed.path.rstrip('/').split('/')[-1]
        code = (parse_qs(parsed.query).get('code') or [''])[0]
        return share_id, code

    @staticmethod
    def response_ok(data: dict) -> bool:
        return str(data.get('code', '')).strip() in {'0', '200'} or data.get('msg', '').lower() in {'', 'ok', 'success'}

    # Business codes that mean "the bearer credential is not accepted".
    # NOTE: Guangya answers missing pagination parameters with HTTP 200 +
    # code 112 ("参数错误") — that is a request-parameter error, NOT an auth
    # rejection (verified live 2026-09-21). Auth rejections are HTTP 401/403
    # only, so this set is intentionally empty; do not add 112 back.
    AUTH_BUSINESS_CODES = frozenset()

    #: get_file_list succeeds only when these pagination fields are present
    #: (live-verified: 200 + code 112 "参数错误" without them, 200 success with
    #: page/pageSize/orderBy/sortType). orderBy=3 + sortType=1 is the default
    #: remote listing order.
    LIST_PAGE_SIZE = 20
    LIST_PAGE_PARAMS: ClassVar[dict[str, int]] = {'orderBy': 3, 'sortType': 1}

    @staticmethod
    def _auth_rejection(exc: BaseException) -> bool:
        """True for HTTP 401/403 or a business-level auth rejection (HTTP 200 + auth code)."""
        if isinstance(exc, httpx.HTTPStatusError):
            if exc.response.status_code in (401, 403):
                return True
            try:
                body = exc.response.json()
            except ValueError:
                return False
            if isinstance(body, dict):
                return str(body.get('code', '')).strip() in GuangyaAdapter.AUTH_BUSINESS_CODES
        return False

    @staticmethod
    def _items(data: dict) -> list[dict]:
        payload = data.get('data') or {}
        items = payload.get('list') if isinstance(payload, dict) else []
        return [item for item in (items or []) if isinstance(item, dict)]

    @staticmethod
    def _has_more(data: dict, *, page: int, page_size: int, item_count: int) -> bool:
        payload = data.get('data') or {}
        if not isinstance(payload, dict):
            payload = {}
        for key in ('hasMore', 'has_more', 'more'):
            value = payload.get(key, data.get(key))
            if value is not None:
                return str(value).strip().lower() in {'1', 'true', 'yes'}
        for key in ('totalPage', 'totalPages', 'pageCount'):
            value = payload.get(key, data.get(key))
            if value is not None:
                try:
                    return page < int(value)
                except (TypeError, ValueError):
                    pass
        for key in ('total', 'totalCount', 'count'):
            value = payload.get(key, data.get(key))
            if value is not None:
                try:
                    return page * page_size < int(value)
                except (TypeError, ValueError):
                    pass
        return item_count >= page_size

    # ------------------------------------------------------------------ #
    # Transport
    # ------------------------------------------------------------------ #

    async def post(self, client: httpx.AsyncClient, url: str, payload: dict, headers: dict) -> dict:
        response = await client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or not self.response_ok(data):
            code = data.get('code') if isinstance(data, dict) else 'invalid-response'
            if isinstance(data, dict) and str(code).strip() in self.AUTH_BUSINESS_CODES:
                # Business-level auth rejection: surface it as an HTTP-status style
                # error carrying the response so _authorized_post can refresh & retry.
                raise httpx.HTTPStatusError(
                    f'guangya auth rejected: code {code}', request=response.request, response=response,
                )
            raise RuntimeError(f'guangya API rejected request: {code}')
        return data

    async def _authorized_post(
        self,
        client: httpx.AsyncClient,
        url: str,
        payload: dict,
        ctx: GuangyaAuthContext,
    ) -> dict:
        """Issue an authenticated POST; on auth rejection (HTTP 401/403 or business
        code 112) refresh once (if possible) and retry once.

        Refresh failure of the credential kind is always surfaced as
        :class:`GuangyaAuthExpiredError`; retryable refresh failures (429, 5xx,
        timeouts) propagate with their own category untouched. A second auth
        rejection after the retry raises :class:`GuangyaAuthExpiredError` —
        never an endless loop.
        """
        headers = self.credential_provider._api_headers(ctx.access_token)
        try:
            return await self.post(client, url, payload, headers)
        except httpx.HTTPStatusError as exc:
            if not self._auth_rejection(exc) or ctx.refreshed_this_chain or not ctx.refresh_token:
                raise
            try:
                access, refresh = await self.credential_provider.refresh_access(client, ctx.refresh_token)
            except GuangyaTransferError as refresh_exc:
                if refresh_exc.category in {
                    TransferErrorCategory.RATE_LIMITED,
                    TransferErrorCategory.NETWORK_TIMEOUT,
                    TransferErrorCategory.NETWORK_ERROR,
                    TransferErrorCategory.REMOTE_5XX,
                }:
                    raise
                raise GuangyaAuthExpiredError() from refresh_exc
            ctx.apply_refreshed(access, refresh)
            if self.credential_store is not None:
                refreshed = {'access_token': access}
                if refresh:
                    refreshed['refresh_token'] = refresh
                await self.credential_store.persist_refresh(self.provider, refreshed)
            headers = self.credential_provider._api_headers(ctx.access_token)
            try:
                return await self.post(client, url, payload, headers)
            except httpx.HTTPStatusError as retry_exc:
                if self._auth_rejection(retry_exc):
                    raise GuangyaAuthExpiredError() from retry_exc
                raise

    async def _list_all_pages(
        self,
        client: httpx.AsyncClient,
        *,
        url: str,
        payload: dict,
        headers: dict,
        page_key: str,
        page_size: int,
        max_pages: int,
    ) -> list[dict]:
        collected: list[dict] = []
        signatures: set[str] = set()
        for page in range(max_pages):
            page_payload = {
                **payload,
                **self.LIST_PAGE_PARAMS,
                page_key: page,
                'page': page,
                'pageSize': page_size,
            }
            data = await self.post(client, url, page_payload, headers)
            items = self._items(data)
            if not items:
                return collected
            signature = json.dumps(items, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)
            if signature in signatures:
                raise RuntimeError(f'guangya pagination repeated page {page}; refusing partial listing')
            signatures.add(signature)
            collected.extend(items)
            if not self._has_more(data, page=page, page_size=page_size, item_count=len(items)):
                return collected
        raise RuntimeError('guangya pagination exceeded configured safety limit')

    async def _list_share_items_recursive(
        self,
        client: httpx.AsyncClient,
        *,
        access_token: str,
        headers: dict,
        page_size: int = 100,
        max_pages: int = 50,
        max_depth: int = 3,
        max_items: int = 500,
    ) -> tuple[list[dict], int, bool]:
        """Recursively enumerate a public share through read-only list APIs only.

        ``max_*`` limits deliberately return a truncated diagnostic rather than
        claiming the root has no videos. No transfer/write endpoint is reachable
        from this helper.
        """
        collected_files: list[dict] = []
        directories = 0
        truncated = False
        dir_queue: list[tuple[str | None, int]] = [(None, 0)]
        while dir_queue:
            parent_id, depth = dir_queue.pop(0)
            req_payload: dict = {'accessToken': access_token}
            if parent_id is not None:
                req_payload['parentId'] = parent_id
            items = await self._list_all_pages(
                client,
                url=f'{self.api_base}/nd.bizuserres.s/v1/get_share_page_files_list',
                payload=req_payload,
                headers=headers,
                page_key='page',
                page_size=page_size,
                max_pages=max_pages,
            )
            for item in items:
                if len(collected_files) + directories >= max_items:
                    return collected_files, directories, True
                if item.get('resType') == 2:
                    directories += 1
                    if depth < max_depth:
                        file_id = self._item_id(item)
                        if file_id:
                            dir_queue.append((file_id, depth + 1))
                    elif depth >= max_depth:
                        truncated = True
                else:
                    collected_files.append(item)
        return collected_files, directories, truncated

    async def _list_share_files_recursive(
        self,
        client: httpx.AsyncClient,
        *,
        access_token: str,
        headers: dict,
        page_size: int = 100,
        max_pages: int = 50,
        max_depth: int = 3,
        max_items: int = 500,
    ) -> list[dict]:
        files, _directories, _truncated = await self._list_share_items_recursive(
            client,
            access_token=access_token,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            max_depth=max_depth,
            max_items=max_items,
        )
        return files

    async def _readback_until_verified(
        self,
        client: httpx.AsyncClient,
        *,
        target_id: str,
        ctx: GuangyaAuthContext,
        expected: set[str],
        attempts: int,
        interval_seconds: float,
    ) -> tuple[bool, set[str]]:
        """Poll the target directory until every *expected* (non-empty) name is present."""
        observed: set[str] = set()
        for attempt in range(attempts):
            items = await self._list_all_pages(
                client,
                url=f'{self.api_base}/userres/v1/file/get_file_list',
                payload={'parentId': target_id},
                headers=self.credential_provider._api_headers(ctx.access_token),
                page_key='pageNum',
                page_size=200,
                max_pages=100,
            )
            observed = {str(item.get('name') or item.get('fileName')) for item in items if item.get('resType') != 2}
            if expected and expected.issubset(observed):
                return True, observed
            if attempt + 1 < attempts:
                await asyncio.sleep(interval_seconds)
        return False, observed

    @staticmethod
    def _item_id(item: dict) -> str:
        return str(item.get('fileId') or item.get('id') or '').strip()

    async def _list_folder_items(
        self,
        client: httpx.AsyncClient,
        *,
        parent_id: str,
        ctx: GuangyaAuthContext,
    ) -> list[dict]:
        """Authorized folder listing sharing the one-and-only 401 refresh path.

        Pages through the remote directory with the full pagination payload
        (parentId/page/pageSize/orderBy/sortType); every page goes through
        _authorized_post so a 401 anywhere still refreshes exactly once.
        """
        collected: list[dict] = []
        signatures: set[str] = set()
        for page in range(100):
            data = await self._authorized_post(
                client,
                f'{self.api_base}/nd.bizuserres.s/v1/file/get_file_list',
                {'parentId': parent_id, 'page': page, 'pageSize': self.LIST_PAGE_SIZE, **self.LIST_PAGE_PARAMS},
                ctx,
            )
            items = self._items(data)
            if not items:
                break
            signature = json.dumps(items, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)
            if signature in signatures:
                raise RuntimeError('guangya folder pagination repeated page; refusing partial listing')
            signatures.add(signature)
            collected.extend(items)
            if not self._has_more(data, page=page, page_size=self.LIST_PAGE_SIZE, item_count=len(items)):
                break
        return collected

    async def _ensure_directory(
        self,
        client: httpx.AsyncClient,
        *,
        parent_id: str,
        name: str,
        ctx: GuangyaAuthContext,
    ) -> str:
        """Return one exact child directory, creating it only when absent."""
        existing = [
            item for item in await self._list_folder_items(client, parent_id=parent_id, ctx=ctx)
            if item.get('resType') == 2 and str(item.get('name') or item.get('fileName') or '') == name
        ]
        if len(existing) > 1:
            raise RuntimeError(f'ambiguous remote directory {name!r} below {parent_id}')
        if existing:
            folder_id = self._item_id(existing[0])
            if folder_id:
                return folder_id
            raise RuntimeError(f'remote directory {name!r} has no file ID')
        await self._authorized_post(
            client,
            f'{self.api_base}/nd.bizuserres.s/v1/file/create_dir',
            {'dirName': name, 'parentId': parent_id},
            ctx,
        )
        created = [
            item for item in await self._list_folder_items(client, parent_id=parent_id, ctx=ctx)
            if item.get('resType') == 2 and str(item.get('name') or item.get('fileName') or '') == name
        ]
        if len(created) != 1:
            raise RuntimeError(f'could not verify exactly one created remote directory {name!r}')
        folder_id = self._item_id(created[0])
        if not folder_id:
            raise RuntimeError(f'created remote directory {name!r} has no file ID')
        return folder_id

    async def _prepare_destination_layout(
        self,
        client: httpx.AsyncClient,
        *,
        payload: dict,
        root_id: str,
        ctx: GuangyaAuthContext,
    ) -> tuple[str, str]:
        """Create season layout or move one verified ongoing series into its final root."""
        series_name = str(payload.get('series_folder_name') or '').strip()
        season_name = str(payload.get('season_folder_name') or '').strip()
        if not series_name or not season_name:
            return root_id, root_id

        promotion_source_id = str(payload.get('promotion_source_series_folder_id') or '').strip()
        if promotion_source_id:
            root_items = await self._list_folder_items(client, parent_id=root_id, ctx=ctx)
            collisions = [
                item for item in root_items
                if item.get('resType') == 2 and str(item.get('name') or item.get('fileName') or '') == series_name
                and self._item_id(item) != promotion_source_id
            ]
            if collisions:
                raise RuntimeError(f'completed destination already contains series directory {series_name!r}')
            await self._authorized_post(
                client,
                f'{self.api_base}/nd.bizuserres.s/v1/file/move_file',
                {'fileIds': [promotion_source_id], 'parentId': root_id},
                ctx,
            )
            root_items = await self._list_folder_items(client, parent_id=root_id, ctx=ctx)
            moved = [item for item in root_items if self._item_id(item) == promotion_source_id and item.get('resType') == 2]
            if len(moved) != 1:
                raise RuntimeError('could not verify moved ongoing series directory in completed root')
            current_name = str(moved[0].get('name') or moved[0].get('fileName') or '')
            if current_name != series_name:
                await self._authorized_post(
                    client,
                    f'{self.api_base}/nd.bizuserres.s/v1/file/rename',
                    {'fileId': promotion_source_id, 'newName': series_name},
                    ctx,
                )
                root_items = await self._list_folder_items(client, parent_id=root_id, ctx=ctx)
                renamed = [
                    item for item in root_items
                    if self._item_id(item) == promotion_source_id
                    and str(item.get('name') or item.get('fileName') or '') == series_name
                ]
                if len(renamed) != 1:
                    raise RuntimeError('could not verify completed series directory rename')
            series_id = promotion_source_id
        else:
            series_id = await self._ensure_directory(client, parent_id=root_id, name=series_name, ctx=ctx)
        season_id = await self._ensure_directory(client, parent_id=series_id, name=season_name, ctx=ctx)
        return series_id, season_id

    async def refresh_access_token(self, client: httpx.AsyncClient, refresh_token: str) -> str | None:
        """One-shot initial refresh (no access token yet). Subclasses may override."""
        access, _ = await self.credential_provider.refresh_access(client, refresh_token)
        return access

    # ------------------------------------------------------------------ #
    # Read-only / transfer entry points
    # ------------------------------------------------------------------ #

    async def list_directories(self, *, auth_token: str, parent_id: str) -> list[dict]:
        """Read direct child directories only; this method never restores or moves data."""
        auth = str(auth_token or '').strip()
        root_id = str(parent_id or '').strip()
        if not auth or not root_id:
            raise ValueError('auth_token and parent_id are required for directory inspection')
        ctx = context_from_auth_ref(auth)
        timeout = httpx.Timeout(30)
        async with httpx.AsyncClient(timeout=timeout) as client:
            if not ctx.access_token:
                if not ctx.refresh_token:
                    raise GuangyaTransferError(TransferErrorCategory.AUTH_INVALID, 'no access or refresh token available')
                access = await self.refresh_access_token(client, ctx.refresh_token)
                if not access:
                    raise GuangyaTransferError(TransferErrorCategory.AUTH_INVALID, 'access token is missing and refresh failed')
                ctx.apply_refreshed(access, None)
                if self.credential_store is not None:
                    await self.credential_store.persist_refresh(self.provider, {'access_token': access})
            items = await self._list_folder_items(client, parent_id=root_id, ctx=ctx)
        return [item for item in items if item.get('resType') == 2]

    async def inspect_share(
        self,
        *,
        share_url: str,
        max_depth: int = 3,
        max_items: int = 500,
        max_pages: int = 50,
    ) -> dict:
        """Recursively read a public share only; no restore/move/create API is used."""
        share_id, code = self.share_parts(share_url)
        if not share_id:
            raise GuangyaTransferError(TransferErrorCategory.INVALID_SHARE, 'share URL has no share id')
        timeout = httpx.Timeout(30)
        async with httpx.AsyncClient(timeout=timeout) as client:
            token_data = await self.post(
                client,
                f'{self.api_base}/nd.bizuserres.s/v1/get_share_access_token',
                {'shareId': share_id, 'code': code},
                self.credential_provider._api_headers(),
            )
            access_token = (token_data.get('data') or {}).get('accessToken')
            if not access_token:
                raise GuangyaTransferError(TransferErrorCategory.INVALID_SHARE, 'share access token missing')
            files, directories, truncated = await self._list_share_items_recursive(
                client,
                access_token=access_token,
                headers=self.credential_provider._api_headers(),
                page_size=100,
                max_pages=max_pages,
                max_depth=max_depth,
                max_items=max_items,
            )
        named_files = [
            {'name': str(item.get('name') or item.get('fileName') or ''), 'file_id': self._item_id(item)}
            for item in files
        ]
        video_files = [item for item in named_files if is_video_filename(item['name'])]
        return {
            'share_accessible': True,
            'share_readable': True,
            'share_id': share_id,
            'items': len(files) + directories,
            'directories': directories,
            'files': named_files,
            'video_files': video_files,
            'video_names': [item['name'] for item in video_files],
            'truncated': truncated,
            'errors': [],
            'error_code': 'SHARE_VALID',
        }

    async def transfer(self, payload: dict) -> TransferOutcome:
        if not self.write_enabled:
            raise TransferNotAllowed('cloud writes are disabled for development/test mode')
        if payload.get('skip_readback'):
            return TransferOutcome(False, False, error='readback verification is mandatory')
        operation = str(payload.get('operation') or 'transfer').strip().lower()
        share_url = str(payload.get('share_url') or '')
        target_id = str(payload.get('target_folder_id') or '')
        auth = str(payload.get('auth_token') or payload.get('auth_ref') or '')
        if not target_id or not auth:
            return TransferOutcome(False, False, error='target_folder_id、auth_token are required')
        if operation != 'promote' and not share_url:
            return TransferOutcome(False, False, error='share_url is required for a restore transfer')
        ctx = context_from_auth_ref(auth)
        timeout = httpx.Timeout(float(payload.get('timeout_seconds', 30)))
        page_size = int(payload.get('page_size', 100))
        max_pages = int(payload.get('max_pages', 1000))
        async with httpx.AsyncClient(timeout=timeout) as client:
            if not ctx.access_token:
                if not ctx.refresh_token:
                    return TransferOutcome(False, False, error='access token is missing and no refresh token is available')
                access = await self.refresh_access_token(client, ctx.refresh_token)
                if not access:
                    return TransferOutcome(False, False, error='access token is missing and refresh failed')
                ctx.apply_refreshed(access, None)
                if self.credential_store is not None:
                    await self.credential_store.persist_refresh(self.provider, {'access_token': access})
            if operation == 'promote':
                series_id, season_id = await self._prepare_destination_layout(
                    client,
                    payload=payload,
                    root_id=target_id,
                    ctx=ctx,
                )
                return TransferOutcome(
                    True,
                    True,
                    remote_folder_id=season_id,
                    remote_series_folder_id=series_id,
                    remote_destination_kind=str(payload.get('destination_kind') or '') or None,
                )
            share_id, code = self.share_parts(share_url)
            token_data = await self.post(
                client,
                f'{self.api_base}/nd.bizuserres.s/v1/get_share_access_token',
                {'shareId': share_id, 'code': code},
                self.credential_provider._api_headers(),
            )
            access_token = (token_data.get('data') or {}).get('accessToken')
            if not access_token:
                return TransferOutcome(False, False, error='share access token missing')
            items = await self._list_share_files_recursive(
                client,
                access_token=access_token,
                headers=self.credential_provider._api_headers(),
                page_size=page_size,
                max_pages=max_pages,
            )
            expected = {str(item).strip() for item in payload.get('expected_files') or [] if str(item).strip()}
            video_files = [
                item for item in items
                if item.get('resType') != 2
                and is_video_filename(str(item.get('name') or item.get('fileName') or ''))
            ]
            if not video_files:
                raise NoVideoFilesError()
            if expected:
                selected = [item for item in video_files if str(item.get('name') or item.get('fileName')) in expected]
                selected_names = {str(item.get('name') or item.get('fileName')) for item in selected}
                if selected_names != expected:
                    raise GuangyaTransferError(
                        TransferErrorCategory.EPISODE_MISMATCH,
                        'share listing did not contain every expected file',
                    )
            else:
                selected = video_files
                selected_names = {str(item.get('name') or item.get('fileName')) for item in selected}
            verification_expected = expected or selected_names
            if not verification_expected:
                raise NoVideoFilesError()
            ids = list(dict.fromkeys(str(item.get('fileId') or item.get('id')) for item in selected if item.get('fileId') or item.get('id')))
            if not ids:
                return TransferOutcome(False, False, error='no video files selected from share')
            series_id, transfer_target_id = await self._prepare_destination_layout(
                client,
                payload=payload,
                root_id=target_id,
                ctx=ctx,
            )
            await self._authorized_post(
                client,
                f'{self.api_base}/nd.bizuserres.s/v1/restore_share',
                {'accessToken': access_token, 'fileIds': ids, 'parentId': transfer_target_id},
                ctx,
            )
            verified, observed = await self._readback_until_verified(
                client,
                target_id=transfer_target_id,
                ctx=ctx,
                expected=verification_expected,
                attempts=max(1, int(payload.get('verify_attempts', 5))),
                interval_seconds=max(0, float(payload.get('verify_interval_seconds', 2))),
            )
            if not verified:
                raise ReadbackVerificationError()
            return TransferOutcome(
                True,
                True,
                remote_folder_id=transfer_target_id,
                remote_files=tuple(sorted(observed)),
                remote_series_folder_id=series_id,
                remote_destination_kind=str(payload.get('destination_kind') or '') or None,
            )
