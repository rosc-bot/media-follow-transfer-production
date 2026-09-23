from __future__ import annotations

import asyncio
import json
import logging
import re
import socket
import time
from typing import ClassVar
from urllib.parse import parse_qs, urlparse

import httpx

from app.core.exceptions import TransferNotAllowed
from app.follow.episode_keys import season_identity
from app.follow.physical_cloud_inventory import PhysicalCloudInventoryScanner
from app.follow.promotion import promotion_readback_decision
from app.transfer.adapters import BaseAdapter
from app.transfer.errors import (
    FileSelectionError,
    GuangyaAuthExpiredError,
    GuangyaTransferError,
    NoVideoFilesError,
    PromotionUnverifiedError,
    ReadbackVerificationError,
    RenameUnverifiedError,
    TransferErrorCategory,
)
from app.transfer.file_selection import (
    SelectionMode,
    assert_selection_scope,
    select_files,
)
from app.transfer.guangya_auth import (
    GuangyaAuthContext,
    GuangyaCredentialProvider,
    GuangyaCredentialStore,
    _safe_exception_message,
    _trace_stage,
    context_from_auth_ref,
    is_video_filename,
)
from app.transfer.rename import build_rename_plan
from app.transfer.status import TransferOutcome

logger = logging.getLogger(__name__)
API_REQUEST_TIMEOUT = httpx.Timeout(15.0, connect=6.0, read=10.0, write=5.0, pool=2.0)


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
        started = time.monotonic()
        trace_state = {'stage': ''}

        async def trace(event_name: str, _info: dict, _state=trace_state) -> None:
            if str(event_name).endswith('.started'):
                traced_stage = _trace_stage(event_name)
                if traced_stage:
                    _state['stage'] = traced_stage

        try:
            response = await client.post(
                url,
                json=payload,
                headers=headers,
                timeout=API_REQUEST_TIMEOUT,
                extensions={'trace': trace},
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            if isinstance(exc, httpx.PoolTimeout):
                stage = 'CONNECTION_POOL_WAIT'
            elif isinstance(exc, httpx.WriteTimeout):
                stage = 'REQUEST_WRITE'
            elif isinstance(exc, httpx.ReadTimeout):
                stage = 'RESPONSE_READ'
            else:
                stage = trace_state['stage'] or 'TCP_CONNECT'
            if (
                stage == 'TCP_CONNECT'
                and isinstance(client, httpx.AsyncClient)
                and isinstance(exc, (httpx.ConnectTimeout, httpx.ConnectError))
            ):
                host = urlparse(url).hostname
                if host:
                    try:
                        await asyncio.wait_for(
                            asyncio.get_running_loop().getaddrinfo(host, 443, type=socket.SOCK_STREAM),
                            timeout=2.0,
                        )
                    except TimeoutError:
                        stage = 'DNS_TIMEOUT'
                    except OSError:
                        stage = 'DNS_RESOLUTION'
            elapsed_ms = round((time.monotonic() - started) * 1000)
            logger.warning(
                'Guangya request failed endpoint_type=api stage=%s elapsed_ms=%s exception_type=%s exception_message=%s',
                stage,
                elapsed_ms,
                type(exc).__name__,
                _safe_exception_message(exc),
            )
            raise
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

    async def _list_share_pages(
        self,
        client: httpx.AsyncClient,
        *,
        payload: dict,
        headers: dict,
        page_size: int,
        max_pages: int,
    ) -> list[dict]:
        """List public-share pages, treating an implicit repeated page as EOF.

        Public share API deployments have been observed to repeat page zero when
        the page is full but omit ``hasMore``. For diagnostics this is a valid
        finite listing; distinct repeated content still cannot be silently used.
        """
        collected: list[dict] = []
        signatures: set[str] = set()
        for page in range(max_pages):
            page_payload = {**payload, **self.LIST_PAGE_PARAMS, 'page': page, 'pageSize': page_size}
            data = await self.post(
                client,
                f'{self.api_base}/nd.bizuserres.s/v1/get_share_page_files_list',
                page_payload,
                headers,
            )
            items = self._items(data)
            if not items:
                return collected
            signature = json.dumps(items, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)
            if signature in signatures:
                metadata = (data.get('data') or {}) if isinstance(data, dict) else {}
                total = metadata.get('total', data.get('total')) if isinstance(data, dict) else None
                if page > 0 and (
                    not any(
                        key in metadata or key in data
                        for key in ('hasMore', 'has_more', 'more', 'totalPage', 'totalPages', 'pageCount')
                    )
                    or (isinstance(total, int | str) and int(total) <= len(collected))
                ):
                    return collected
                raise RuntimeError(f'guangya share pagination repeated page {page}; refusing partial listing')
            signatures.add(signature)
            collected.extend(items)
            if not self._has_more(data, page=page, page_size=page_size, item_count=len(items)):
                return collected
        raise RuntimeError('guangya share pagination exceeded configured safety limit')

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
            items = await self._list_share_pages(
                client,
                payload=req_payload,
                headers=headers,
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
        selection_mode: SelectionMode | None = None,
        preexisting_names: set[str] | None = None,
    ) -> tuple[bool, set[str]]:
        """Poll until selected names exist and reject newly leaked video scope."""
        observed: set[str] = set()
        existing = {str(name).strip() for name in (preexisting_names or set()) if str(name).strip()}
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
            observed = {
                str(item.get('name') or item.get('fileName')).strip()
                for item in items
                if item.get('resType') != 2 and str(item.get('name') or item.get('fileName')).strip()
            }
            if selection_mode in {SelectionMode.SINGLE_EPISODE, SelectionMode.MISSING_EPISODES}:
                unexpected = {
                    name for name in (observed - existing) if is_video_filename(name)
                } - expected
                if unexpected:
                    raise FileSelectionError(
                        'TRANSFER_SCOPE_VIOLATION',
                        f'readback observed newly restored files outside selected episode: {sorted(unexpected)}',
                        category=TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
                    )
            if expected and expected.issubset(observed):
                return True, observed
            if attempt + 1 < attempts:
                await asyncio.sleep(interval_seconds)
        return False, observed

    @staticmethod
    def _video_records(items: list[dict]) -> list[dict]:
        return [
            item for item in items
            if item.get('resType') != 2
            and str(item.get('name') or item.get('fileName') or '').strip()
        ]

    async def _rename_selected_verified_files(
        self,
        client: httpx.AsyncClient,
        *,
        target_id: str,
        ctx: GuangyaAuthContext,
        records: list[dict],
        selected_names: set[str],
        payload: dict,
    ) -> tuple[list[dict], str]:
        """Rename only the selected readback records, then verify by listing again."""

        layout_title = str(payload.get('title') or '').strip()
        if not layout_title:
            layout_title = re.sub(
                r'\s*\{\s*tmdb(?:id)?[-:_= ]*\d+\s*\}\s*|\s*【完结】\s*$',
                '',
                str(payload.get('series_folder_name') or ''),
                flags=re.IGNORECASE,
            ).strip()
        # Low-level adapter callers that did not carry a verified media identity
        # cannot safely synthesize a standard Chinese name.  Preserve the
        # verified source name and leave the production orchestrator to hydrate
        # the title before this stage; this is a fail-closed naming no-op, not a
        # broad rename fallback.
        if not layout_title:
            payload['rename_status'] = 'RENAME_SKIPPED_NO_VERIFIED_TITLE'
            payload['rename_verified'] = True
            return records, 'RENAME_SKIPPED_NO_VERIFIED_TITLE'

        selected_records = [
            item for item in self._video_records(records)
            if str(item.get('name') or item.get('fileName') or '').strip() in selected_names
        ]
        selected_ids = [self._item_id(item) for item in selected_records if self._item_id(item)]
        if len(selected_ids) != len(selected_records) or len(selected_records) != len(selected_names):
            raise RenameUnverifiedError(
                'selected verified files are not uniquely present in target readback',
                remote_folder_id=target_id,
                remote_records=records,
            )
        persisted_plan = payload.get('rename_plan') or {}
        persisted_operations = [
            item for item in persisted_plan.get('operations') or []
            if isinstance(item, dict)
        ]
        if persisted_operations:
            by_id = {self._item_id(item): item for item in self._video_records(records) if self._item_id(item)}
            persisted_final = True
            persisted_old = True
            for operation in persisted_operations:
                current = by_id.get(str(operation.get('file_id') or '').strip())
                current_name = str(current.get('name') or current.get('fileName') or '').strip() if current else ''
                persisted_final = persisted_final and current_name == str(operation.get('new_name') or '').strip()
                persisted_old = persisted_old and current_name == str(operation.get('old_name') or '').strip()
            if persisted_final:
                payload['rename_status'] = 'RENAME_VERIFIED'
                payload['rename_verified'] = True
                return records, 'RENAME_VERIFIED'
            if not persisted_old:
                raise RenameUnverifiedError(
                    'rename retry readback is neither the persisted source nor final name',
                    remote_folder_id=target_id,
                    remote_records=records,
                )
        lifecycle_verified = bool(payload.get('lifecycle_verified'))
        plan = build_rename_plan(
            records,
            selected_file_ids=selected_ids,
            destination_kind=str(payload.get('destination_kind') or 'ongoing'),
            lifecycle_verified=lifecycle_verified,
            title=layout_title,
            year=payload.get('year'),
            tmdb_id=payload.get('tmdb_id'),
            season=payload.get('season'),
            episode_key=(payload.get('episode_keys') or [None])[0],
            version_key=payload.get('version_key'),
            media_type=str(payload.get('media_type') or 'tv'),
            series_status=payload.get('series_status') or payload.get('tmdb_series_status'),
            content_complete=payload.get('content_complete'),
            total_episodes=payload.get('total_episodes'),
            collected_episodes=payload.get('collected_episodes'),
            inventory_count=payload.get('inventory_count'),
            cloud_count=payload.get('cloud_count'),
            active_transfer_count=payload.get('active_transfer_count'),
            aliases=list(payload.get('aliases') or []),
            episode_keys_by_file_id=dict(payload.get('selected_episode_by_file_id') or {}),
        )
        payload['rename_plan'] = plan.as_dict()
        if plan.status in {
            'RENAME_SKIPPED_KEEP_EXISTING_NAME',
            'RENAME_SKIPPED_STANDARD_CHINESE',
        }:
            payload['rename_status'] = plan.status
            payload['rename_verified'] = True
            return records, plan.status
        if plan.status != 'RENAME_READY':
            raise RenameUnverifiedError(
                plan.reason or plan.status,
                remote_folder_id=target_id,
                remote_records=records,
            )
        payload['execution_stage'] = 'RENAMING'
        for operation in plan.operations:
            await self._authorized_post(
                client,
                f'{self.api_base}/nd.bizuserres.s/v1/file/rename',
                {'fileId': operation.file_id, 'newName': operation.new_name},
                ctx,
            )
        after = await self._list_folder_items(client, parent_id=target_id, ctx=ctx)
        after_records = self._video_records(after)
        by_id = {self._item_id(item): item for item in after_records if self._item_id(item)}
        for operation in plan.operations:
            item = by_id.get(operation.file_id)
            current_name = str(item.get('name') or item.get('fileName') or '').strip() if item else ''
            if item is None or current_name != operation.new_name:
                raise RenameUnverifiedError(
                    f'rename readback missing final name {operation.new_name}',
                    remote_folder_id=target_id,
                    remote_records=after_records,
                )
            if current_name == operation.old_name:
                raise RenameUnverifiedError(
                    f'rename readback retained old name {operation.old_name}',
                    remote_folder_id=target_id,
                    remote_records=after_records,
                )
        payload['rename_status'] = 'RENAME_VERIFIED'
        payload['rename_verified'] = True
        return after_records, 'RENAME_VERIFIED'

    @staticmethod
    def _selected_sizes(records: list[dict], selected_names: set[str]) -> dict[str, int]:
        sizes: dict[str, int] = {}
        for item in records:
            name = str(item.get('name') or item.get('fileName') or '').strip()
            if name not in selected_names:
                continue
            try:
                sizes[name] = int(item.get('size') or item.get('fileSize') or 0)
            except (TypeError, ValueError):
                sizes[name] = 0
        return sizes

    @staticmethod
    def _record_names(records: list[dict]) -> tuple[str, ...]:
        return tuple(sorted({
            str(item.get('name') or item.get('fileName') or '').strip()
            for item in records
            if str(item.get('name') or item.get('fileName') or '').strip()
        }))

    @staticmethod
    def _record_payload(records: list[dict]) -> list[dict]:
        return [
            {
                'file_id': str(item.get('fileId') or item.get('id') or '').strip(),
                'name': str(item.get('name') or item.get('fileName') or '').strip(),
                'size': item.get('size') or item.get('fileSize') or 0,
            }
            for item in records
            if str(item.get('name') or item.get('fileName') or '').strip()
        ]

    @staticmethod
    def _verified_episode_file_records(records: list[dict], episode_by_file_id: dict[str, str]) -> list[dict]:
        by_id: dict[str, dict] = {}
        for item in records:
            file_id = GuangyaAdapter._item_id(item)
            if file_id:
                if file_id in by_id:
                    raise FileSelectionError(
                        'TRANSFER_SCOPE_VIOLATION',
                        'readback contains duplicate remote file IDs for episode mapping',
                        category=TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
                    )
                by_id[file_id] = item
        verified = []
        for raw_id, raw_key in episode_by_file_id.items():
            file_id = str(raw_id).strip()
            key = str(raw_key).strip()
            item = by_id.get(file_id)
            if not file_id or not key or item is None:
                raise FileSelectionError(
                    'TRANSFER_SCOPE_VIOLATION',
                    'verified readback does not contain every selected episode file ID',
                    category=TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
                )
            name = str(item.get('name') or item.get('fileName') or '').strip()
            verified.append({
                'episode_key': key,
                'file_id': file_id,
                'file_name': name,
                'size': item.get('size') or item.get('fileSize') or 0,
            })
        return verified

    @staticmethod
    def _layout_prefix(payload: dict) -> str | None:
        canonical = str(payload.get('inventory_prefix') or payload.get('remote_rel_path_prefix') or '').strip('/')
        if canonical:
            return canonical
        series = str(payload.get('series_folder_name') or '').strip()
        season = str(payload.get('season_folder_name') or '').strip()
        if not series:
            return None
        return f'{series}/{season}'.strip('/') if season else series

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
        seen_ids: set[str] = set()
        expected_total: int | None = None
        expected_pages: int | None = None
        more_expected = False
        for page in range(100):
            data = await self._authorized_post(
                client,
                f'{self.api_base}/nd.bizuserres.s/v1/file/get_file_list',
                {'parentId': parent_id, 'page': page, 'pageSize': self.LIST_PAGE_SIZE, **self.LIST_PAGE_PARAMS},
                ctx,
            )
            meta = data.get('data') if isinstance(data.get('data'), dict) else {}
            raw_total = meta.get('total', data.get('total'))
            if raw_total is not None:
                try:
                    page_total = int(raw_total)
                except (TypeError, ValueError) as exc:
                    raise RuntimeError('guangya folder pagination returned invalid total') from exc
                if page_total < 0 or (expected_total is not None and expected_total != page_total):
                    raise RuntimeError('guangya folder pagination returned inconsistent total')
                expected_total = page_total
            raw_pages = next((meta.get(key, data.get(key)) for key in ('totalPage', 'totalPages', 'pageCount') if meta.get(key, data.get(key)) is not None), None)
            if raw_pages is not None:
                try:
                    page_count = int(raw_pages)
                except (TypeError, ValueError) as exc:
                    raise RuntimeError('guangya folder pagination returned invalid page count') from exc
                if page_count < 0 or (expected_pages is not None and expected_pages != page_count):
                    raise RuntimeError('guangya folder pagination returned inconsistent page count')
                expected_pages = page_count
            items = self._items(data)
            if not items:
                if expected_total is not None and len(collected) != expected_total:
                    raise RuntimeError('guangya folder pagination ended before reported total')
                if more_expected or (expected_pages is not None and page < expected_pages):
                    raise RuntimeError('guangya folder pagination ended before reported next page')
                break
            signature = json.dumps(items, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)
            if signature in signatures:
                raise RuntimeError('guangya folder pagination repeated page; refusing partial listing')
            signatures.add(signature)
            for item in items:
                item_id = self._item_id(item)
                if not item_id:
                    raise RuntimeError('guangya folder listing returned an item without an ID')
                if item_id in seen_ids:
                    raise RuntimeError('guangya folder pagination repeated an item ID')
                seen_ids.add(item_id)
            collected.extend(items)
            if expected_total is not None:
                if len(collected) > expected_total:
                    raise RuntimeError('guangya folder pagination exceeded reported total')
                if len(collected) == expected_total:
                    break
                if len(items) < self.LIST_PAGE_SIZE:
                    raise RuntimeError('SHORT_PAGE_BEFORE_TOTAL')
                more_expected = True
                continue
            explicit_more = next((meta.get(key, data.get(key)) for key in ('hasMore', 'has_more', 'more') if meta.get(key, data.get(key)) is not None), None)
            if explicit_more is not None:
                more_expected = str(explicit_more).strip().lower() in {'1', 'true', 'yes'}
                if more_expected:
                    continue
                break
            more_expected = page + 1 < expected_pages if expected_pages is not None else len(items) >= self.LIST_PAGE_SIZE
            if more_expected:
                continue
            break
        else:
            raise RuntimeError('guangya folder pagination exceeded configured safety limit')
        return collected

    @staticmethod
    def _tmdb_identity_from_directory_name(name: str) -> int | None:
        match = re.search(r'\{\s*tmdb(?:id)?[-:_= ]*(\d+)\s*\}', str(name or ''), re.IGNORECASE)
        return int(match.group(1)) if match else None

    @staticmethod
    def _unidentified_title_key(name: str) -> str:
        value = str(name or '')
        value = re.sub(r'\{\s*tmdb(?:id)?[-:_= ]*\d+\s*\}', ' ', value, flags=re.IGNORECASE)
        value = re.sub(r'【完结】', ' ', value)
        value = re.sub(r'\(\s*\d{4}\s*\)', ' ', value)
        value = re.sub(r'(?i)(?<![A-Za-z0-9])(?:4k|2160p|1080p|720p|web[- ]?dl|blu[- ]?ray|bluray|remux|hdr|dv)(?![A-Za-z0-9])', ' ', value)
        value = re.sub(r'(?i)(?:S\s*0*\d+|Season\s*0*\d+|第[0-9一二三四五六七八九十]+季)', ' ', value)
        return re.sub(r'[^\w]+', '', value, flags=re.UNICODE).casefold()

    async def _find_tmdb_series_roots_readonly(
        self,
        client: httpx.AsyncClient,
        *,
        root_id: str,
        root_kind: str,
        media_root: str,
        tmdb_id: int,
        expected_name: str,
        ctx: GuangyaAuthContext,
        max_depth: int = 6,
        max_directories: int = 5000,
        max_items: int = 50000,
    ) -> tuple[list[dict], list[dict]]:
        """Find all direct TMDB roots below one lifecycle root using list-only APIs."""
        root_id = str(root_id or '').strip()
        if not root_id:
            raise RuntimeError('TMDB_ROOT_SCAN_ROOT_ID_MISSING')
        root_label = '未完结追新' if root_kind == 'ongoing' else '影视转存总目录'
        pending = [(root_id, [root_label], 0)]
        visited: set[str] = set()
        matches: list[dict] = []
        unidentified: list[dict] = []
        item_count = 0
        while pending:
            parent_id, parent_parts, depth = pending.pop(0)
            if parent_id in visited:
                raise RuntimeError('TMDB_ROOT_SCAN_DIRECTORY_CYCLE_OR_ALIAS')
            visited.add(parent_id)
            if len(visited) > max_directories:
                raise RuntimeError('TMDB_ROOT_SCAN_MAX_DIRECTORIES_EXCEEDED')
            items = await self._list_folder_items(client, parent_id=parent_id, ctx=ctx)
            for item in items:
                item_count += 1
                if item_count > max_items:
                    raise RuntimeError('TMDB_ROOT_SCAN_MAX_ITEMS_EXCEEDED')
                if item.get('resType') != 2:
                    continue
                folder_id = self._item_id(item)
                name = str(item.get('name') or item.get('fileName') or '').strip()
                if not folder_id or not name:
                    raise RuntimeError('TMDB_ROOT_SCAN_DIRECTORY_ID_OR_NAME_MISSING')
                path_parts = [*parent_parts, name]
                identity = self._tmdb_identity_from_directory_name(name)
                if identity == int(tmdb_id):
                    matches.append({
                        'folder_id': folder_id,
                        'name': name,
                        'parent_id': parent_id,
                        'path': '/'.join(path_parts),
                        'kind': root_kind,
                    })
                    if depth >= max_depth:
                        raise RuntimeError('TMDB_ROOT_SCAN_MAX_DEPTH_EXCEEDED')
                    # Continue through the matching root's folders so a nested
                    # duplicate TMDB root cannot be mistaken for one unique root.
                    pending.append((folder_id, path_parts, depth + 1))
                    continue
                if identity is not None:
                    # A tagged sibling is already a series/movie root; its children
                    # cannot contain another series root identity for this search.
                    continue
                expected_title_key = self._unidentified_title_key(expected_name)
                if name == expected_name or (
                    expected_title_key and self._unidentified_title_key(name) == expected_title_key
                ):
                    unidentified.append({
                        'folder_id': folder_id,
                        'name': name,
                        'parent_id': parent_id,
                        'path': '/'.join(path_parts),
                        'kind': root_kind,
                    })
                    continue
                if parent_id == root_id and name in {'电影', '电视剧'} and media_root and name != media_root:
                    continue
                if depth >= max_depth:
                    raise RuntimeError('TMDB_ROOT_SCAN_MAX_DEPTH_EXCEEDED')
                pending.append((folder_id, path_parts, depth + 1))
        return matches, unidentified

    async def _resolve_tmdb_series_root_readonly(
        self,
        client: httpx.AsyncClient,
        *,
        payload: dict,
        current_root_id: str,
        tmdb_id: int,
        expected_name: str,
        media_root: str,
        ctx: GuangyaAuthContext,
    ) -> dict | None:
        destination_kind = str(payload.get('destination_kind') or '').casefold()
        ongoing_id = str(payload.get('ongoing_root_id') or '').strip()
        completed_id = str(payload.get('completed_root_id') or '').strip()
        if not ongoing_id and destination_kind == 'ongoing':
            ongoing_id = str(current_root_id or '').strip()
        if not completed_id and destination_kind == 'completed':
            completed_id = str(current_root_id or '').strip()
        if not ongoing_id or not completed_id or ongoing_id == completed_id:
            raise FileSelectionError('TMDB_ROOT_SCAN_BOTH_LIFECYCLE_ROOTS_REQUIRED', 'both ongoing and completed root IDs must be configured')
        roots = [('ongoing', ongoing_id), ('completed', completed_id)]
        matches: list[dict] = []
        unidentified: list[dict] = []
        try:
            async with asyncio.timeout(90.0):
                for kind, root_id in roots:
                    found, unknown = await self._find_tmdb_series_roots_readonly(
                        client,
                        root_id=root_id,
                        root_kind=kind,
                        media_root=media_root,
                        tmdb_id=int(tmdb_id),
                        expected_name=expected_name,
                        ctx=ctx,
                    )
                    matches.extend(found)
                    unidentified.extend(unknown)
        except TimeoutError as exc:
            raise RuntimeError('TMDB_ROOT_SCAN_TIMEOUT') from exc
        if len(matches) > 1:
            raise FileSelectionError('DUPLICATE_TMDB_ROOT', f'tmdb_id={int(tmdb_id)} matches={len(matches)}')
        if unidentified:
            raise FileSelectionError('SERIES_ROOT_IDENTITY_UNVERIFIED', f'tmdb_id={int(tmdb_id)} has an untagged title-matching root')
        return matches[0] if matches else None

    @staticmethod
    def _apply_existing_root_path(payload: dict, root: dict, season_name: str | None) -> None:
        parts = [part for part in str(root.get('path') or '').split('/') if part]
        if len(parts) < 2 or parts[-1] != str(root.get('name') or ''):
            raise RuntimeError('TMDB_ROOT_PATH_READBACK_INVALID')
        relative_parts = parts[1:-1]
        root_label = parts[0]
        actual_series_name = str(root['name']).strip()
        payload['series_folder_name'] = actual_series_name
        payload['destination_kind'] = str(root['kind'])
        root_id_key = 'ongoing_root_id' if root['kind'] == 'ongoing' else 'completed_root_id'
        payload['target_folder_id'] = str(payload.get(root_id_key) or payload.get('target_folder_id') or '')
        payload['media_root'] = relative_parts[0] if len(relative_parts) >= 2 else ''
        payload['media_category'] = relative_parts[1] if len(relative_parts) >= 2 else ''
        payload['sub_category'] = payload['media_category']
        item_prefix = '/'.join([*relative_parts, actual_series_name])
        payload['destination_prefix'] = item_prefix
        prefix = '/'.join(part for part in (item_prefix, season_name) if part)
        payload['inventory_prefix'] = prefix
        payload['remote_rel_path_prefix'] = prefix
        archive_parts = [root_label, *relative_parts, actual_series_name]
        if season_name:
            archive_parts.append(season_name)
        payload['archive_directory'] = ' / '.join(archive_parts)

    async def _resolve_season_directory(
        self,
        client: httpx.AsyncClient,
        *,
        series_id: str,
        season: int,
        requested_name: str | None,
        ctx: GuangyaAuthContext,
        payload: dict,
        allow_create: bool = True,
    ) -> str:
        if int(season) <= 0:
            raise FileSelectionError('SEASON_IDENTITY_REQUIRED', 'series transfer requires a positive season identity')
        items = await self._list_folder_items(client, parent_id=series_id, ctx=ctx)
        directories = [item for item in items if item.get('resType') == 2]
        parsed = []
        unknown = []
        for item in directories:
            name = str(item.get('name') or item.get('fileName') or '').strip()
            sid = season_identity(name)
            if sid is not None:
                parsed.append((int(sid), item, name))
            elif re.search(r'(?i)\bS\s*\d+|\bSeason\s*\d+|第.{1,4}季|\d+季', name):
                unknown.append(name)
        if unknown:
            raise FileSelectionError('UNKNOWN_SEASON_FOLDER', f'unrecognized season-like directory names: {sorted(unknown)}')
        matches = [(item, name) for sid, item, name in parsed if sid == int(season)]
        if len(matches) > 1:
            raise FileSelectionError('DUPLICATE_SEASON_ROOT', f'season={int(season)} folder_count={len(matches)}')
        if matches:
            folder_id = self._item_id(matches[0][0])
            if not folder_id:
                raise FileSelectionError('SEASON_FOLDER_ID_MISSING', 'resolved season directory has no remote folder ID')
            payload['season_folder_name'] = matches[0][1]
            return folder_id
        root_level_videos = [
            item for item in items
            if item.get('resType') != 2
            and is_video_filename(str(item.get('name') or item.get('fileName') or ''))
        ]
        if root_level_videos:
            if not parsed and not requested_name and int(season) == 1:
                payload.pop('season_folder_name', None)
                return series_id
            raise FileSelectionError('MIXED_SINGLE_SEASON_LAYOUT', 'root-level video files conflict with season folders')
        if not requested_name and not parsed:
            payload.pop('season_folder_name', None)
            return series_id
        if not allow_create:
            raise RuntimeError(f'PROMOTION_SEASON_FOLDER_MISSING: season={int(season)}')
        create_name = requested_name or f'S{int(season):02d}'
        requested_identity = season_identity(create_name)
        if requested_identity != int(season):
            raise FileSelectionError('SEASON_IDENTITY_MISMATCH', f'requested folder {create_name!r} does not match season {int(season)}')
        folder_id = await self._ensure_directory(client, parent_id=series_id, name=create_name, ctx=ctx)
        payload['season_folder_name'] = create_name
        return folder_id

    async def _ensure_tmdb_series_directory(
        self,
        client: httpx.AsyncClient,
        *,
        parent_id: str,
        name: str,
        tmdb_id: int,
        ctx: GuangyaAuthContext,
    ) -> tuple[str, str]:
        """Reuse one direct child by TMDB identity; never create beside duplicates."""
        items = await self._list_folder_items(client, parent_id=parent_id, ctx=ctx)
        directories = [item for item in items if item.get('resType') == 2]
        matches = [
            item for item in directories
            if self._tmdb_identity_from_directory_name(
                str(item.get('name') or item.get('fileName') or '')
            ) == int(tmdb_id)
        ]
        if len(matches) > 1:
            raise FileSelectionError('DUPLICATE_TMDB_ROOT', f'tmdb_id={int(tmdb_id)} has {len(matches)} direct child roots')
        if matches:
            item = matches[0]
            folder_id = self._item_id(item)
            actual_name = str(item.get('name') or item.get('fileName') or '').strip()
            if not folder_id or not actual_name:
                raise FileSelectionError('SERIES_ROOT_IDENTITY_UNVERIFIED', f'tmdb_id={int(tmdb_id)} matched root has no ID or name')
            return folder_id, actual_name
        exact_unidentified = [
            item for item in directories
            if str(item.get('name') or item.get('fileName') or '').strip() == name
        ]
        if exact_unidentified:
            raise FileSelectionError('SERIES_ROOT_IDENTITY_UNVERIFIED', f'title-only root {name!r} has no TMDB tag for tmdb_id={int(tmdb_id)}')
        folder_id = await self._ensure_directory(
            client,
            parent_id=parent_id,
            name=name,
            ctx=ctx,
        )
        return folder_id, name

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
        """Build root/category/item/season and move promotion roots into that layout."""
        series_name = str(payload.get('series_folder_name') or '').strip()
        season_name = str(payload.get('season_folder_name') or '').strip() or None
        operation = str(payload.get('operation') or 'transfer').strip().casefold()
        media_root_name = str(payload.get('media_root') or '').strip()
        media_category_name = str(payload.get('media_category') or payload.get('sub_category') or '').strip()
        promotion_source_id = str(payload.get('promotion_source_series_folder_id') or '').strip()
        if promotion_source_id and operation != 'promote':
            raise RuntimeError('PROMOTION_OPERATION_REQUIRED')
        tmdb_id = int(payload.get('tmdb_id') or 0)
        media_type = str(payload.get('media_type') or 'tv').casefold()
        is_series = media_type not in {'movie', 'film', '电影'}
        if bool(media_root_name) != bool(media_category_name):
            raise RuntimeError('canonical destination requires both media_root and media_category')
        root_candidate = None
        if series_name and is_series:
            if tmdb_id <= 0:
                raise FileSelectionError('SERIES_ROOT_IDENTITY_UNVERIFIED', 'positive TMDB ID is required before creating a series root')
            root_candidate = await self._resolve_tmdb_series_root_readonly(
                client,
                payload=payload,
                current_root_id=root_id,
                tmdb_id=tmdb_id,
                expected_name=series_name,
                media_root=media_root_name,
                ctx=ctx,
            )
            if promotion_source_id and (
                root_candidate is None
                or root_candidate.get('kind') != 'ongoing'
                or str(root_candidate.get('folder_id')) != promotion_source_id
            ):
                raise PromotionUnverifiedError(
                    f'promotion source folder does not match the unique ongoing TMDB root for {tmdb_id}'
                )
        actual_series_name = series_name
        if root_candidate and not promotion_source_id:
            layout_parent_id = str(root_candidate['parent_id'])
            series_id = str(root_candidate['folder_id'])
            actual_series_name = str(root_candidate['name'])
            payload['destination_category_folder_id'] = layout_parent_id
            payload['destination_kind'] = str(root_candidate['kind'])
            target_key = 'ongoing_root_id' if root_candidate['kind'] == 'ongoing' else 'completed_root_id'
            payload['target_folder_id'] = str(payload.get(target_key) or root_id)
        else:
            layout_parent_id = root_id
            if media_root_name and media_category_name:
                media_root_id = await self._ensure_directory(
                    client, parent_id=root_id, name=media_root_name, ctx=ctx,
                )
                layout_parent_id = await self._ensure_directory(
                    client, parent_id=media_root_id, name=media_category_name, ctx=ctx,
                )
                payload['destination_media_root_folder_id'] = media_root_id
                payload['destination_category_folder_id'] = layout_parent_id
        if not series_name:
            return layout_parent_id, layout_parent_id

        if promotion_source_id:
            if root_candidate is None:
                raise PromotionUnverifiedError(
                    f'promotion source folder does not match the unique ongoing TMDB root for {tmdb_id}'
                )
            root_items = await self._list_folder_items(client, parent_id=layout_parent_id, ctx=ctx)
            identity_matches = [
                item for item in root_items
                if item.get('resType') == 2
                and tmdb_id > 0
                and self._tmdb_identity_from_directory_name(
                    str(item.get('name') or item.get('fileName') or '')
                ) == tmdb_id
            ]
            if any(self._item_id(item) != promotion_source_id for item in identity_matches):
                raise RuntimeError(f'DUPLICATE_TMDB_ROOT: tmdb_id={tmdb_id}')
            collisions = [
                item for item in root_items
                if item.get('resType') == 2
                and str(item.get('name') or item.get('fileName') or '') == series_name
                and self._item_id(item) != promotion_source_id
            ]
            if collisions:
                raise RuntimeError(f'completed destination already contains series directory {series_name!r}')
            if not any(self._item_id(item) == promotion_source_id for item in root_items):
                payload['promotion_stage'] = 'MOVE_SUBMITTED'
                payload['promotion_source_parent_id'] = str(root_candidate['parent_id'])
                payload['promotion_destination_parent_id'] = layout_parent_id
                payload['promotion_series_folder_id'] = promotion_source_id
                await self._authorized_post(
                    client,
                    f'{self.api_base}/nd.bizuserres.s/v1/file/move_file',
                    {'fileIds': [promotion_source_id], 'parentId': layout_parent_id},
                    ctx,
                )
            root_items = await self._list_folder_items(client, parent_id=layout_parent_id, ctx=ctx)
            moved = [item for item in root_items if self._item_id(item) == promotion_source_id and item.get('resType') == 2]
            if len(moved) != 1:
                raise RuntimeError('could not verify moved ongoing series directory in completed category')
            payload['promotion_source_parent_id'] = str(root_candidate['parent_id'])
            payload['promotion_destination_parent_id'] = layout_parent_id
            payload['promotion_original_series_name'] = str(moved[0].get('name') or moved[0].get('fileName') or '')
            await self._verify_promotion_parent_readback(
                client,
                series_id=promotion_source_id,
                source_parent_id=str(root_candidate['parent_id']),
                destination_parent_id=layout_parent_id,
                ctx=ctx,
            )
            payload['promotion_stage'] = 'MOVED'
            payload['promotion_series_folder_id'] = promotion_source_id
            series_id = promotion_source_id
        elif tmdb_id > 0 and str(payload.get('media_type') or 'tv').casefold() not in {'movie', 'film', '电影'}:
            series_id, actual_series_name = await self._ensure_tmdb_series_directory(
                client,
                parent_id=layout_parent_id,
                name=series_name,
                tmdb_id=tmdb_id,
                ctx=ctx,
            )
        else:
            series_id = await self._ensure_directory(client, parent_id=layout_parent_id, name=series_name, ctx=ctx)

        if actual_series_name != series_name and not root_candidate:
            payload['series_folder_name'] = actual_series_name
            canonical_base = '/'.join(
                part for part in (media_root_name, media_category_name, actual_series_name) if part
            )
            canonical_prefix = '/'.join(part for part in (canonical_base, season_name) if part)
            payload['destination_prefix'] = canonical_base
            payload['inventory_prefix'] = canonical_prefix
            payload['remote_rel_path_prefix'] = canonical_prefix
            root_label = '影视转存总目录' if str(payload.get('destination_kind') or '').casefold() == 'completed' else '未完结追新'
            archive_parts = [root_label, media_root_name, media_category_name, actual_series_name]
            if season_name:
                archive_parts.append(season_name)
            payload['archive_directory'] = ' / '.join(part for part in archive_parts if part)

        season_id = series_id
        if is_series:
            try:
                season_number = int(payload.get('season') or season_identity(season_name) or 1)
            except (TypeError, ValueError) as exc:
                raise RuntimeError('SEASON_IDENTITY_REQUIRED') from exc
            season_id = await self._resolve_season_directory(
                client,
                series_id=series_id,
                season=season_number,
                requested_name=None if operation == 'promote' else season_name,
                ctx=ctx,
                payload=payload,
                allow_create=operation != 'promote',
            )
        if root_candidate and not promotion_source_id:
            self._apply_existing_root_path(payload, root_candidate, payload.get('season_folder_name'))
        return series_id, season_id

    async def refresh_access_token(self, client: httpx.AsyncClient, refresh_token: str) -> str | None:
        """One-shot initial refresh (no access token yet). Subclasses may override."""
        access, _ = await self.credential_provider.refresh_access(client, refresh_token)
        return access

    # ------------------------------------------------------------------ #
    # Read-only / transfer entry points
    # ------------------------------------------------------------------ #

    async def inspect_tmdb_series_root_readonly(
        self,
        *,
        auth_token: str,
        target_root_id: str,
        media_root_name: str,
        media_category_name: str,
        tmdb_id: int,
        expected_series_name: str | None = None,
    ) -> dict:
        """Read only the canonical root/category direct-child chain for one TMDB ID."""
        ctx = context_from_auth_ref(str(auth_token or '').strip())
        if not ctx.access_token or not str(target_root_id or '').strip() or int(tmdb_id) <= 0:
            return {'status': 'API_ERROR', 'error': 'READ_ONLY_IDENTITY_OR_AUTH_MISSING', 'series_roots': []}
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(45.0)) as client:
                async def list_readonly_with_retry(parent_id: str) -> list[dict]:
                    for attempt in range(3):
                        try:
                            return await self._list_folder_items(client, parent_id=parent_id, ctx=ctx)
                        except httpx.TimeoutException:
                            if attempt == 2:
                                raise
                            await asyncio.sleep(0.5)
                    raise RuntimeError('READ_ONLY_DIRECTORY_LIST_RETRIES_EXHAUSTED')

                root_items = await list_readonly_with_retry(str(target_root_id))
                media = [
                    item for item in root_items
                    if item.get('resType') == 2
                    and str(item.get('name') or item.get('fileName') or '').strip() == str(media_root_name).strip()
                ]
                if len(media) > 1:
                    return {'status': 'NEEDS_REVIEW', 'error': 'MEDIA_ROOT_AMBIGUOUS', 'series_roots': []}
                if not media:
                    return {'status': 'VERIFIED', 'series_roots': []}
                media_id = self._item_id(media[0])
                if not media_id:
                    return {'status': 'NEEDS_REVIEW', 'error': 'MEDIA_ROOT_ID_MISSING', 'series_roots': []}
                category_items = await list_readonly_with_retry(media_id)
                categories = [
                    item for item in category_items
                    if item.get('resType') == 2
                    and str(item.get('name') or item.get('fileName') or '').strip() == str(media_category_name).strip()
                ]
                if len(categories) > 1:
                    return {'status': 'NEEDS_REVIEW', 'error': 'MEDIA_CATEGORY_AMBIGUOUS', 'series_roots': []}
                if not categories:
                    return {'status': 'VERIFIED', 'series_roots': []}
                category_id = self._item_id(categories[0])
                if not category_id:
                    return {'status': 'NEEDS_REVIEW', 'error': 'MEDIA_CATEGORY_ID_MISSING', 'series_roots': []}
                series_items = await list_readonly_with_retry(category_id)
                directories = [item for item in series_items if item.get('resType') == 2]
                matches = [
                    item for item in directories
                    if self._tmdb_identity_from_directory_name(
                        str(item.get('name') or item.get('fileName') or '')
                    ) == int(tmdb_id)
                ]
                roots = [
                    {'folder_id': self._item_id(item), 'name': str(item.get('name') or item.get('fileName') or '').strip()}
                    for item in matches
                ]
                if len(roots) > 1:
                    return {'status': 'DUPLICATE_TMDB_ROOT', 'error': 'DUPLICATE_TMDB_ROOT', 'series_roots': roots}
                if not roots and expected_series_name and any(
                    str(item.get('name') or item.get('fileName') or '').strip() == expected_series_name
                    for item in directories
                ):
                    return {'status': 'NEEDS_REVIEW', 'error': 'SERIES_ROOT_IDENTITY_UNVERIFIED', 'series_roots': []}
                if roots and not roots[0]['folder_id']:
                    return {'status': 'NEEDS_REVIEW', 'error': 'SERIES_ROOT_ID_MISSING', 'series_roots': roots}
                return {'status': 'VERIFIED', 'series_roots': roots}
        except Exception as exc:  # noqa: BLE001 - provider read failures are fail-closed
            return {'status': 'API_ERROR', 'error': type(exc).__name__, 'series_roots': []}

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
                page_size=20,
                max_pages=max_pages,
                max_depth=max_depth,
                max_items=max_items,
            )
        named_files = [
            {
                'name': str(item.get('name') or item.get('fileName') or ''),
                'file_id': self._item_id(item),
                'size': item.get('size') or item.get('fileSize') or item.get('sizeBytes') or item.get('bytes') or 0,
            }
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

    async def _read_series_tree(
        self,
        client: httpx.AsyncClient,
        *,
        series_id: str,
        ctx: GuangyaAuthContext,
        max_depth: int = 6,
    ) -> list[dict]:
        """Read a moved series root recursively without any write endpoint."""

        queue: list[tuple[str, str, int]] = [(series_id, '', 0)]
        files: list[dict] = []
        while queue:
            parent_id, relative, depth = queue.pop(0)
            if depth > max_depth:
                raise PromotionUnverifiedError(
                    'promotion readback exceeded season hierarchy depth',
                    series_folder_id=series_id,
                    remote_records=files,
                )
            items = await self._list_folder_items(client, parent_id=parent_id, ctx=ctx)
            for item in items:
                name = str(item.get('name') or item.get('fileName') or '').strip()
                if not name:
                    continue
                item_copy = dict(item)
                item_copy['_relative_path'] = f'{relative}/{name}'.strip('/')
                if item.get('resType') == 2:
                    child_id = self._item_id(item)
                    if child_id:
                        queue.append((child_id, item_copy['_relative_path'], depth + 1))
                else:
                    files.append(item_copy)
        return files

    @staticmethod
    def _completed_folder_name(name: str) -> str:
        base = re.sub(r'(?:\s*【完结】)+\s*$', '', str(name or '')).strip()
        if not base:
            raise PromotionUnverifiedError('verified promotion title is missing')
        return f'{base}【完结】'

    @staticmethod
    def _replace_path_component(value: object, old_name: str, new_name: str) -> str:
        parts = [part for part in str(value or '').strip('/').split('/') if part]
        indices = [index for index, part in enumerate(parts) if part == old_name]
        if indices:
            parts[indices[-1]] = new_name
        return '/'.join(parts)

    async def _verify_promotion_parent_readback(
        self,
        client: httpx.AsyncClient,
        *,
        series_id: str,
        source_parent_id: str,
        destination_parent_id: str,
        ctx: GuangyaAuthContext,
    ) -> str:
        source_parent_id = str(source_parent_id or '').strip()
        destination_parent_id = str(destination_parent_id or '').strip()
        if not source_parent_id or not destination_parent_id or source_parent_id == destination_parent_id:
            raise PromotionUnverifiedError(
                'promotion source/destination parent fence is missing or ambiguous',
                series_folder_id=series_id,
            )
        destination_items = await self._list_folder_items(
            client, parent_id=destination_parent_id, ctx=ctx,
        )
        destination_matches = [
            item for item in destination_items
            if item.get('resType') == 2 and self._item_id(item) == series_id
        ]
        source_items = await self._list_folder_items(
            client, parent_id=source_parent_id, ctx=ctx,
        )
        source_matches = [
            item for item in source_items
            if item.get('resType') == 2 and self._item_id(item) == series_id
        ]
        if len(destination_matches) != 1 or source_matches:
            raise PromotionUnverifiedError(
                'promotion folder-id parent readback is not unique or source still exists',
                series_folder_id=series_id,
                remote_records=destination_matches + source_matches,
            )
        return str(destination_matches[0].get('name') or destination_matches[0].get('fileName') or '').strip()

    async def _verify_promotion_readback(
        self,
        client: httpx.AsyncClient,
        *,
        series_id: str,
        source_series_id: str,
        completed_root_id: str,
        ctx: GuangyaAuthContext,
        payload: dict,
    ) -> tuple[list[dict], dict[str, list[str]]]:
        expected_raw = payload.get('promotion_expected_files_by_season') or {}
        expected = {
            str(season): [str(name).strip() for name in files if str(name).strip()]
            for season, files in expected_raw.items()
            if isinstance(files, (list, tuple))
        }
        if not expected:
            gate = payload.get('promotion_gate') or {}
            expected = {
                str(season): [str(name).strip() for name in files if str(name).strip()]
                for season, files in (gate.get('expected_files_by_season') or {}).items()
                if isinstance(files, (list, tuple))
            }
        if not expected:
            raise PromotionUnverifiedError(
                'promotion expected file map is missing; refusing a blind move success',
                series_folder_id=series_id,
            )
        records = await self._read_series_tree(client, series_id=series_id, ctx=ctx)
        observed: dict[str, list[str]] = {}
        for item in records:
            path = str(item.get('_relative_path') or '')
            path_seasons = {
                sid for part in path.split('/')[:-1]
                if (sid := season_identity(part)) is not None
            }
            file_name = str(item.get('name') or item.get('fileName') or '').strip()
            file_match = re.search(r'(?i)S(\d{1,3})E\d{1,4}', file_name)
            file_season = int(file_match.group(1)) if file_match else None
            if len(path_seasons) > 1 or (path_seasons and file_season and file_season not in path_seasons):
                raise PromotionUnverifiedError(
                    'promotion readback contains conflicting season identities',
                    series_folder_id=series_id,
                    remote_records=records,
                )
            season_number = next(iter(path_seasons), None) or file_season
            if season_number is None:
                season_number = season_identity(str(payload.get('season_folder_name') or '')) or 1
            season_key = f'S{int(season_number):02d}'
            observed.setdefault(season_key, []).append(file_name)
        source_parent_id = str(
            payload.get('promotion_source_parent_id') or payload.get('ongoing_root_id') or ''
        ).strip()
        if not source_parent_id:
            raise PromotionUnverifiedError(
                'promotion source parent ID is missing',
                series_folder_id=series_id,
            )
        source_root_items = await self._list_folder_items(
            client,
            parent_id=source_parent_id,
            ctx=ctx,
        )
        source_exists = any(self._item_id(item) == source_series_id for item in source_root_items)
        decision = promotion_readback_decision(
            move_returned=True,
            expected_files_by_season=expected,
            observed_files_by_season=observed,
            source_root_exists=source_exists,
            destination_conflict=False,
        )
        if decision != 'PROMOTION_COMPLETED':
            raise PromotionUnverifiedError(
                f'{decision}: completed root readback is incomplete',
                series_folder_id=series_id,
                remote_records=records,
            )
        return records, observed

    async def scan_series_root_readonly(
        self,
        *,
        auth_token: str,
        tmdb_id: int,
        series_root_id: str,
        relevant_seasons: list[int] | tuple[int, ...],
        timeout_seconds: float = 30.0,
        max_depth: int = 6,
        max_items: int = 5000,
        page_size: int = 100,
        rate_limit_seconds: float = 0.05,
    ) -> dict:
        """Run the bounded PhysicalCloudInventoryScanner with list-only APIs."""
        ctx = context_from_auth_ref(str(auth_token or '').strip())
        if not ctx.access_token:
            return {
                'tmdb_id': int(tmdb_id),
                'series_root_id': str(series_root_id or ''),
                'scan_status': 'API_ERROR',
                'error': 'READ_ONLY_SCAN_REQUIRES_ACCESS_TOKEN',
                'cloud_episode_keys_by_season': {},
                'file_count': 0,
                'scan_watermark': None,
            }
        timeout = httpx.Timeout(max(0.1, float(timeout_seconds)))
        async with httpx.AsyncClient(timeout=timeout) as client:
            async def list_page(parent_id: str, page: int, requested_page_size: int):
                if page > 0:
                    return {'data': {'items': [], 'has_more': False}}
                items = await self._list_folder_items(client, parent_id=parent_id, ctx=ctx)
                if rate_limit_seconds:
                    await asyncio.sleep(rate_limit_seconds)
                return {'data': {'items': items, 'has_more': False}}

            result = await PhysicalCloudInventoryScanner(
                list_page,
                timeout_seconds=timeout_seconds,
                max_depth=max_depth,
                max_items=max_items,
                page_size=page_size,
                rate_limit_seconds=rate_limit_seconds,
            ).scan(
                tmdb_id=int(tmdb_id),
                series_root_id=str(series_root_id or ''),
                relevant_seasons=relevant_seasons,
            )
        return result.as_dict()

    async def inspect_completed_root_conflict_readonly(
        self,
        *,
        auth_token: str,
        completed_root_id: str,
        tmdb_id: int,
        title: str,
        media_root: str = '电视剧',
        expected_series_name: str | None = None,
        ongoing_root_id: str | None = None,
        page_size: int = 100,
    ) -> dict:
        """Recursively search only directory roots for this TMDB ID; never write."""
        del page_size  # directory listings use the provider's verified page size
        ctx = context_from_auth_ref(str(auth_token or '').strip())
        if not ctx.access_token:
            return {'status': 'UNVERIFIED', 'conflict': True, 'error': 'READ_ONLY_SCAN_REQUIRES_ACCESS_TOKEN'}
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                try:
                    async with asyncio.timeout(90.0):
                        candidates = []
                        unidentified = []
                        roots_to_scan = []
                        if ongoing_root_id:
                            roots_to_scan.append(('ongoing', str(ongoing_root_id).strip()))
                        roots_to_scan.append(('completed', str(completed_root_id).strip()))
                        if not all(root_id for _kind, root_id in roots_to_scan) or len({root_id for _kind, root_id in roots_to_scan}) != len(roots_to_scan):
                            return {'status': 'UNVERIFIED', 'conflict': True, 'error': 'LIFECYCLE_ROOT_CONFIGURATION_INVALID', 'series_roots': []}
                        for root_kind, root_id in roots_to_scan:
                            roots, unknown = await self._find_tmdb_series_roots_readonly(
                                client,
                                root_id=root_id,
                                root_kind=root_kind,
                                media_root=str(media_root or '电视剧'),
                                tmdb_id=int(tmdb_id),
                                expected_name=str(expected_series_name or title or '').strip(),
                                ctx=ctx,
                            )
                            candidates.extend(roots)
                            unidentified.extend(unknown)
                except TimeoutError:
                    return {'status': 'UNVERIFIED', 'conflict': True, 'error': 'COMPLETED_ROOT_SCAN_TIMEOUT'}
            if unidentified:
                return {'status': 'UNVERIFIED', 'conflict': True, 'error': 'SERIES_ROOT_IDENTITY_UNVERIFIED', 'series_roots': []}
            if len(candidates) > 1:
                return {'status': 'DUPLICATE_TMDB_ROOT', 'conflict': True, 'error': 'DUPLICATE_TMDB_ROOT', 'series_roots': candidates}
            completed_matches = [root for root in candidates if root.get('kind') == 'completed']
            return {
                'status': 'VERIFIED',
                'conflict': bool(completed_matches),
                'matched_direct_children': completed_matches,
                'series_roots': candidates,
                'recursive_scan': True,
            }
        except Exception as exc:  # noqa: BLE001 - provider/read scan errors fail closed
            return {'status': 'UNVERIFIED', 'conflict': True, 'error': type(exc).__name__, 'series_roots': []}

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
            resume_stage = str(payload.get('execution_stage') or '').upper()
            if operation != 'promote' and resume_stage in {'RESTORED', 'RESTORE_VERIFIED', 'RENAMING', 'RENAME_VERIFIED'}:
                resume_target = str(payload.get('remote_folder_id') or target_id).strip()
                if not resume_target:
                    raise RenameUnverifiedError('rename retry has no verified remote folder fence')
                resume_plan = payload.get('rename_plan') or {}
                resume_names = {
                    str(value).strip()
                    for value in (payload.get('selected_verified_names') or payload.get('selected_file_names') or payload.get('expected_files') or [])
                    if str(value).strip()
                }
                for operation_row in resume_plan.get('operations') or []:
                    if isinstance(operation_row, dict):
                        for key in ('old_name', 'new_name'):
                            if str(operation_row.get(key) or '').strip():
                                resume_names.add(str(operation_row[key]).strip())
                records = self._video_records(await self._list_folder_items(client, parent_id=resume_target, ctx=ctx))
                current_selected = {
                    str(item.get('name') or item.get('fileName') or '').strip()
                    for item in records
                    if str(item.get('name') or item.get('fileName') or '').strip() in resume_names
                }
                if not current_selected:
                    raise RenameUnverifiedError(
                        'rename retry cannot locate selected verified files',
                        remote_folder_id=resume_target,
                        remote_records=records,
                    )
                try:
                    final_records, rename_status = await self._rename_selected_verified_files(
                        client,
                        target_id=resume_target,
                        ctx=ctx,
                        records=records,
                        selected_names=current_selected,
                        payload=payload,
                    )
                except RenameUnverifiedError as exc:
                    payload['execution_stage'] = 'RENAMING'
                    payload['remote_folder_id'] = resume_target
                    payload['verified_remote_records'] = self._record_payload(exc.remote_records or records)
                    raise
                selected_ids = {
                    str(value).strip()
                    for value in (payload.get('selected_verified_file_ids') or [])
                    if str(value).strip()
                }
                if not selected_ids:
                    selected_ids = {
                        self._item_id(item)
                        for item in final_records
                        if str(item.get('name') or item.get('fileName') or '').strip() in current_selected
                    }
                final_selected = [
                    str(item.get('name') or item.get('fileName') or '').strip()
                    for item in final_records
                    if self._item_id(item) in selected_ids
                ]
                payload['execution_stage'] = 'RENAME_VERIFIED'
                payload['selected_file_names'] = list(dict.fromkeys(final_selected))
                payload['expected_files'] = list(dict.fromkeys(final_selected))
                payload['selected_file_sizes'] = self._selected_sizes(final_records, set(final_selected))
                payload['remote_file_records'] = self._record_payload(final_records)
                verified_episode_files = self._verified_episode_file_records(
                    final_records,
                    dict(payload.get('selected_episode_by_file_id') or {}),
                )
                if str(payload.get('selection_mode') or '').upper() == SelectionMode.MISSING_EPISODES.value:
                    expected_episode_keys = list(payload.get('selected_episode_keys') or [])
                    observed_episode_keys = [str(item.get('episode_key') or '') for item in verified_episode_files]
                    if len(observed_episode_keys) != len(expected_episode_keys) or set(observed_episode_keys) != set(expected_episode_keys):
                        raise FileSelectionError(
                            'TRANSFER_SCOPE_VIOLATION',
                            'resume readback does not verify every selected missing episode exactly once',
                            category=TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
                        )
                payload['verified_episode_files'] = verified_episode_files
                if (prefix := self._layout_prefix(payload)):
                    payload['remote_rel_path_prefix'] = prefix
                return TransferOutcome(
                    True,
                    True,
                    remote_folder_id=resume_target,
                    remote_files=self._record_names(final_records),
                    remote_file_records=tuple(self._record_payload(final_records)),
                    rename_status=rename_status,
                    verified_episode_files=tuple(verified_episode_files),
                )

            if operation == 'promote':
                source_series_id = str(payload.get('promotion_source_series_folder_id') or '').strip()
                if not source_series_id:
                    raise PromotionUnverifiedError('promotion source series folder ID is required')
                promotion_stage = str(payload.get('promotion_stage') or '').upper()
                if promotion_stage in {'MOVE_SUBMITTED', 'MOVED'}:
                    # Once a move request may have reached Guangya, retries are
                    # strictly readback/rename-only and never resubmit move_file.
                    series_id = str(payload.get('promotion_series_folder_id') or '').strip()
                    season_id = str(payload.get('promotion_season_folder_id') or '').strip() or series_id
                    if not series_id:
                        raise PromotionUnverifiedError('promotion readback fence has no series folder ID')
                    if not payload.get('promotion_source_parent_id') or not payload.get('promotion_destination_parent_id'):
                        raise PromotionUnverifiedError('promotion readback fence has no verified source/destination parents')
                else:
                    series_id, season_id = await self._prepare_destination_layout(
                        client,
                        payload=payload,
                        root_id=target_id,
                        ctx=ctx,
                    )
                    payload['promotion_stage'] = 'MOVED'
                    payload['promotion_series_folder_id'] = series_id
                    payload['promotion_season_folder_id'] = season_id
                try:
                    promotion_records, observed_by_season = await self._verify_promotion_readback(
                        client,
                        series_id=series_id,
                        source_series_id=source_series_id,
                        completed_root_id=target_id,
                        ctx=ctx,
                        payload=payload,
                    )
                    destination_parent_id = str(payload.get('promotion_destination_parent_id') or '').strip()
                    source_parent_id = str(payload.get('promotion_source_parent_id') or '').strip()
                    current_root_name = await self._verify_promotion_parent_readback(
                        client,
                        series_id=series_id,
                        source_parent_id=source_parent_id,
                        destination_parent_id=destination_parent_id,
                        ctx=ctx,
                    )
                    tmdb_id = int(payload.get('tmdb_id') or 0)
                    if tmdb_id <= 0:
                        raise PromotionUnverifiedError('promotion TMDB identity is missing', series_folder_id=series_id)
                    series_base_name = str(payload.get('series_folder_name') or current_root_name).strip()
                    desired_name = self._completed_folder_name(series_base_name)
                    identity_readback = await self._resolve_tmdb_series_root_readonly(
                        client,
                        payload=payload,
                        current_root_id=str(payload.get('completed_root_id') or target_id),
                        tmdb_id=tmdb_id,
                        expected_name=series_base_name,
                        media_root=str(payload.get('media_root') or ''),
                        ctx=ctx,
                    )
                    if (
                        identity_readback is None
                        or str(identity_readback.get('folder_id')) != series_id
                        or identity_readback.get('kind') != 'completed'
                    ):
                        raise PromotionUnverifiedError(
                            'promotion TMDB root identity is not unique in completed storage',
                            series_folder_id=series_id,
                        )
                    if current_root_name != desired_name:
                        await self._authorized_post(
                            client,
                            f'{self.api_base}/nd.bizuserres.s/v1/file/rename',
                            {'fileId': series_id, 'newName': desired_name},
                            ctx,
                        )
                    final_name = await self._verify_promotion_parent_readback(
                        client,
                        series_id=series_id,
                        source_parent_id=source_parent_id,
                        destination_parent_id=destination_parent_id,
                        ctx=ctx,
                    )
                    if final_name != desired_name:
                        raise PromotionUnverifiedError(
                            'completed marker rename readback did not match the required name',
                            series_folder_id=series_id,
                        )
                    final_identity = await self._resolve_tmdb_series_root_readonly(
                        client,
                        payload=payload,
                        current_root_id=str(payload.get('completed_root_id') or target_id),
                        tmdb_id=tmdb_id,
                        expected_name=series_base_name,
                        media_root=str(payload.get('media_root') or ''),
                        ctx=ctx,
                    )
                    if (
                        final_identity is None
                        or str(final_identity.get('folder_id')) != series_id
                        or final_identity.get('kind') != 'completed'
                    ):
                        raise PromotionUnverifiedError(
                            'completed folder ID is not unique after marker rename',
                            series_folder_id=series_id,
                        )
                except PromotionUnverifiedError as exc:
                    payload['promotion_stage'] = 'MOVED'
                    payload['promotion_series_folder_id'] = series_id
                    payload['promotion_season_folder_id'] = season_id
                    payload['promotion_status'] = 'PROMOTION_UNVERIFIED'
                    payload['promotion_readback'] = {
                        'status': 'PROMOTION_UNVERIFIED',
                        'error_type': type(exc).__name__,
                        'observed_by_season': observed_by_season if 'observed_by_season' in locals() else {},
                    }
                    raise
                except Exception as exc:
                    payload['promotion_stage'] = 'MOVED'
                    payload['promotion_series_folder_id'] = series_id
                    payload['promotion_season_folder_id'] = season_id
                    payload['promotion_status'] = 'PROMOTION_UNVERIFIED'
                    payload['promotion_readback'] = {
                        'status': 'PROMOTION_UNVERIFIED',
                        'error_type': type(exc).__name__,
                        'observed_by_season': observed_by_season if 'observed_by_season' in locals() else {},
                    }
                    raise PromotionUnverifiedError(
                        f'promotion readback/marker verification failed: {type(exc).__name__}',
                        series_folder_id=series_id,
                    ) from exc
                payload['series_folder_name'] = desired_name
                item_prefix = '/'.join(
                    part for part in (
                        str(payload.get('media_root') or '').strip('/'),
                        str(payload.get('media_category') or payload.get('sub_category') or '').strip('/'),
                        desired_name,
                    ) if part
                )
                season_name = str(payload.get('season_folder_name') or '').strip()
                payload['destination_kind'] = 'completed'
                payload['destination_prefix'] = item_prefix
                payload['inventory_prefix'] = '/'.join(part for part in (item_prefix, season_name) if part)
                payload['remote_rel_path_prefix'] = payload['inventory_prefix']
                archive_parts = [
                    '影视转存总目录',
                    str(payload.get('media_root') or '').strip('/'),
                    str(payload.get('media_category') or payload.get('sub_category') or '').strip('/'),
                    desired_name,
                ]
                if season_name:
                    archive_parts.append(season_name)
                payload['archive_directory'] = ' / '.join(part for part in archive_parts if part)
                payload['promotion_status'] = 'PROMOTION_COMPLETED'
                payload['promotion_marker_status'] = 'COMPLETION_MARKER_VERIFIED'
                payload['promotion_readback'] = {
                    'status': 'PROMOTION_COMPLETED',
                    'folder_id': series_id,
                    'folder_name': desired_name,
                    'folder_id_unique': True,
                    'ongoing_source_absent': True,
                    'marker_verified': True,
                    'observed_by_season': observed_by_season,
                }
                return TransferOutcome(
                    True,
                    True,
                    remote_folder_id=season_id,
                    remote_files=self._record_names(promotion_records),
                    remote_file_records=tuple(self._record_payload(promotion_records)),
                    remote_series_folder_id=series_id,
                    remote_destination_kind='completed',
                    promotion_status='PROMOTION_COMPLETED',
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
            video_files = [
                item for item in items
                if item.get('resType') != 2
                and is_video_filename(str(item.get('name') or item.get('fileName') or ''))
            ]
            if not video_files:
                raise NoVideoFilesError()
            snapshot = payload.get('selection_snapshot') or payload.get('hydrated_selection') or {}
            if not snapshot and payload.get('selected_file_ids'):
                snapshot = {
                    'selection_mode': payload.get('selection_mode'),
                    'selected_file_ids': list(payload.get('selected_file_ids') or []),
                    'selected_file_names': list(payload.get('selected_file_names') or payload.get('expected_files') or []),
                }
            snapshot_mode = snapshot.get('selection_mode') if isinstance(snapshot, dict) else None
            requested_ids = snapshot.get('selected_file_ids') if isinstance(snapshot, dict) else None
            requested_names = snapshot.get('selected_file_names') if isinstance(snapshot, dict) else None
            result = select_files(
                video_files,
                selection_mode=payload.get('selection_mode') or snapshot_mode,
                episode_keys=list(payload.get('episode_keys') or []),
                target_episode_key=payload.get('episode_key'),
                season=payload.get('season'),
                expected_files=list(payload.get('expected_files') or []),
                selected_file_ids=requested_ids,
                selected_file_names=requested_names,
            )
            if snapshot:
                if snapshot_mode and str(snapshot_mode).upper() != result.selection_mode.value:
                    raise FileSelectionError(
                        'SELECTION_SNAPSHOT_MODE_CHANGED',
                        'selection snapshot mode does not match the live share selection mode',
                    )
                snapshot_ids = [str(value).strip() for value in (requested_ids or []) if str(value).strip()]
                snapshot_names = [str(value).strip() for value in (requested_names or []) if str(value).strip()]
                if result.selection_mode is SelectionMode.MISSING_EPISODES:
                    snapshot_episode_map = {
                        str(key).strip(): str(value).strip()
                        for key, value in (snapshot.get('episode_file_map') or {}).items()
                        if str(key).strip() and str(value).strip()
                    }
                    if (
                        not snapshot_ids
                        or not snapshot_names
                        or snapshot_ids != result.selected_file_ids
                        or snapshot_names != result.selected_file_names
                        or (snapshot_episode_map and snapshot_episode_map != result.episode_file_map)
                    ):
                        raise FileSelectionError(
                            'SELECTION_SNAPSHOT_CHANGED',
                            'live missing-episode map no longer matches the verified preflight snapshot',
                        )
                else:
                    if snapshot_ids:
                        result.selected_file_ids = snapshot_ids
                    if snapshot_names:
                        result.selected_file_names = snapshot_names
                    result.decision = 'HYDRATED_SELECTION'
            assert_selection_scope(result)
            selected_by_id = {
                str(item.get('fileId') or item.get('id') or '').strip(): item
                for item in video_files
                if str(item.get('fileId') or item.get('id') or '').strip()
            }
            selected = [selected_by_id[file_id] for file_id in result.selected_file_ids if file_id in selected_by_id]
            if len(selected) != len(result.selected_file_ids):
                raise FileSelectionError(
                    'TRANSFER_SCOPE_VIOLATION',
                    'one or more selected file IDs disappeared from the live share listing',
                    category=TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
                )
            selected_names = {str(item.get('name') or item.get('fileName') or '') for item in selected}
            if selected_names != set(result.selected_file_names):
                raise FileSelectionError(
                    'TRANSFER_SCOPE_VIOLATION',
                    'selected file IDs and selected file names disagree',
                    category=TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
                )
            verification_expected = set(result.selected_file_names)
            if not verification_expected:
                raise FileSelectionError(
                    'CANARY_ABORTED_SELECTION_TOO_BROAD',
                    'selected file names are empty; refusing an unscoped restore',
                )
            payload['selection_mode'] = result.selection_mode.value
            payload['expected_files'] = sorted(verification_expected)
            payload['selection_result'] = result.to_dict()
            source_episode_by_id = {
                str(file_id).strip(): str(key).strip()
                for key, file_id in result.episode_file_map.items()
                if str(file_id).strip() and str(key).strip()
            }
            if result.selection_mode is SelectionMode.MISSING_EPISODES and len(source_episode_by_id) != len(result.selected_file_ids):
                raise FileSelectionError(
                    'MISSING_EPISODE_MAP_INCOMPLETE',
                    'missing-episode selection does not map every selected file ID to one episode',
                )
            if source_episode_by_id:
                payload['selected_episode_keys'] = list(result.selected_episode_keys)
                payload['episode_keys'] = list(result.selected_episode_keys)
                payload['selected_episode_by_file_id'] = source_episode_by_id
            ids = list(dict.fromkeys(result.selected_file_ids))
            if not ids:
                raise FileSelectionError(
                    'FILE_ID_MISSING',
                    'selected files do not contain remote file IDs',
                )
            series_id, transfer_target_id = await self._prepare_destination_layout(
                client,
                payload=payload,
                root_id=target_id,
                ctx=ctx,
            )
            before_items = await self._list_folder_items(client, parent_id=transfer_target_id, ctx=ctx)
            preexisting_names = {
                str(item.get('name') or item.get('fileName') or '').strip()
                for item in before_items
                if item.get('resType') != 2 and str(item.get('name') or item.get('fileName') or '').strip()
            }
            if result.selection_mode is SelectionMode.MISSING_EPISODES:
                destination_overlap = sorted(verification_expected & preexisting_names)
                if destination_overlap:
                    raise FileSelectionError(
                        'DESTINATION_EPISODE_OVERLAP',
                        f'selected missing episodes already have exact destination filenames: {destination_overlap}',
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
                selection_mode=result.selection_mode,
                preexisting_names=preexisting_names,
            )
            if not verified:
                raise ReadbackVerificationError()
            readback_records = self._video_records(
                await self._list_folder_items(client, parent_id=transfer_target_id, ctx=ctx)
            )
            selected_target_ids = {
                self._item_id(item)
                for item in readback_records
                if str(item.get('name') or item.get('fileName') or '').strip() in verification_expected
                and self._item_id(item)
            }
            selected_target_records = [
                item for item in readback_records
                if str(item.get('name') or item.get('fileName') or '').strip() in verification_expected
            ]
            if result.selection_mode is SelectionMode.MISSING_EPISODES and len(selected_target_records) != len(verification_expected):
                raise FileSelectionError(
                    'TRANSFER_SCOPE_VIOLATION',
                    'target readback does not contain exactly one record per selected missing episode',
                    category=TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
                )
            source_id_by_name = {
                str(item.get('name') or item.get('fileName') or '').strip(): str(item.get('fileId') or item.get('id') or '').strip()
                for item in selected
                if str(item.get('name') or item.get('fileName') or '').strip()
            }
            source_episode_by_id = dict(payload.get('selected_episode_by_file_id') or {})
            target_episode_by_id = {}
            for item in selected_target_records:
                name = str(item.get('name') or item.get('fileName') or '').strip()
                source_id = source_id_by_name.get(name)
                episode_key = str(source_episode_by_id.get(source_id) or '').strip()
                target_id = self._item_id(item) or source_id
                if episode_key and target_id:
                    target_episode_by_id[target_id] = episode_key
                if not self._item_id(item) and source_id:
                    selected_target_ids.add(source_id)
            if result.selection_mode is SelectionMode.MISSING_EPISODES:
                if set(target_episode_by_id.values()) != set(result.selected_episode_keys) or len(target_episode_by_id) != len(result.selected_episode_keys):
                    raise FileSelectionError(
                        'TRANSFER_SCOPE_VIOLATION',
                        'restore readback episode map is not one-to-one with selected episodes',
                        category=TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
                    )
                payload['selected_episode_by_file_id'] = target_episode_by_id
            payload['execution_stage'] = 'RESTORE_VERIFIED'
            payload['remote_folder_id'] = transfer_target_id
            payload['selected_verified_names'] = sorted(verification_expected)
            payload['selected_verified_file_ids'] = sorted(selected_target_ids)
            payload['verified_remote_records'] = self._record_payload(readback_records)
            try:
                final_records, rename_status = await self._rename_selected_verified_files(
                    client,
                    target_id=transfer_target_id,
                    ctx=ctx,
                    records=readback_records,
                    selected_names=verification_expected,
                    payload=payload,
                )
            except RenameUnverifiedError as exc:
                payload['execution_stage'] = 'RENAMING'
                payload['remote_folder_id'] = transfer_target_id
                payload['verified_remote_records'] = self._record_payload(exc.remote_records or readback_records)
                raise
            final_selected = [
                str(item.get('name') or item.get('fileName') or '').strip()
                for item in final_records
                if self._item_id(item) in selected_target_ids
                or (not self._item_id(item) and str(item.get('name') or item.get('fileName') or '').strip() in verification_expected)
            ]
            payload['execution_stage'] = 'RENAME_VERIFIED'
            payload['selected_file_names'] = list(dict.fromkeys(final_selected))
            payload['expected_files'] = list(dict.fromkeys(final_selected))
            payload['selected_file_sizes'] = self._selected_sizes(final_records, set(final_selected))
            payload['remote_file_records'] = self._record_payload(final_records)
            verified_episode_files = self._verified_episode_file_records(final_records, target_episode_by_id)
            expected_episode_keys = list(payload.get('selected_episode_keys') or [])
            if result.selection_mode is SelectionMode.MISSING_EPISODES:
                verified_keys = [str(item.get('episode_key') or '') for item in verified_episode_files]
                if len(verified_keys) != len(expected_episode_keys) or set(verified_keys) != set(expected_episode_keys):
                    raise FileSelectionError(
                        'TRANSFER_SCOPE_VIOLATION',
                        'final renamed readback does not verify every selected episode exactly once',
                        category=TransferErrorCategory.TRANSFER_SCOPE_VIOLATION,
                    )
            payload['verified_episode_files'] = verified_episode_files
            if (prefix := self._layout_prefix(payload)):
                payload['remote_rel_path_prefix'] = prefix
            return TransferOutcome(
                True,
                True,
                remote_folder_id=transfer_target_id,
                remote_files=self._record_names(final_records),
                remote_file_records=tuple(self._record_payload(final_records)),
                remote_series_folder_id=series_id,
                remote_destination_kind=str(payload.get('destination_kind') or '') or None,
                rename_status=rename_status,
                verified_episode_files=tuple(verified_episode_files),
            )
