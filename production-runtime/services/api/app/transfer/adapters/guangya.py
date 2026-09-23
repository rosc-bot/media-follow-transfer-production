from __future__ import annotations

import asyncio
import json
import re
from typing import ClassVar
from urllib.parse import parse_qs, urlparse

import httpx

from app.core.exceptions import TransferNotAllowed
from app.follow.completed_root_conflict import CompletedRootConflictScanner
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
    context_from_auth_ref,
    is_video_filename,
)
from app.transfer.rename import build_rename_plan
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
            if selection_mode is SelectionMode.SINGLE_EPISODE:
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
    def _layout_prefix(payload: dict) -> str | None:
        series = str(payload.get('series_folder_name') or '').strip()
        season = str(payload.get('season_folder_name') or '').strip()
        if not series or not season:
            return None
        return f'{series}/{season}'

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
            season_match = re.search(r'(?i)(S\d{2}|Season\s*\d+|第\d+季)', path)
            if season_match:
                token = season_match.group(1)
                if token.casefold().startswith('season') or token.startswith('第'):
                    number_match = re.search(r'\d+', token)
                    season_key = f"S{int(number_match.group()):02d}" if number_match else 'S01'
                else:
                    season_key = token.upper()
            else:
                season_key = str(payload.get('season_folder_name') or 'S01')
            observed.setdefault(season_key, []).append(
                str(item.get('name') or item.get('fileName') or '').strip()
            )
        source_root_items = await self._list_folder_items(
            client,
            parent_id=str(payload.get('ongoing_root_id') or '').strip(),
            ctx=ctx,
        ) if str(payload.get('ongoing_root_id') or '').strip() else []
        source_exists = not str(payload.get('ongoing_root_id') or '').strip() or any(
            self._item_id(item) == source_series_id for item in source_root_items
        )
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
                return await self.post(
                    client,
                    f'{self.api_base}/nd.bizuserres.s/v1/file/get_file_list',
                    {
                        'parentId': parent_id,
                        'page': page,
                        'pageSize': requested_page_size,
                        **self.LIST_PAGE_PARAMS,
                    },
                    self.credential_provider._api_headers(ctx.access_token),
                )

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
        page_size: int = 100,
    ) -> dict:
        """Inspect only direct completed-root children; never recurse or write."""
        ctx = context_from_auth_ref(str(auth_token or '').strip())
        if not ctx.access_token:
            return {'status': 'UNVERIFIED', 'conflict': True, 'error': 'READ_ONLY_SCAN_REQUIRES_ACCESS_TOKEN'}
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                items: list[dict] = []
                for page in range(100):
                    data = await self.post(
                        client,
                        f'{self.api_base}/nd.bizuserres.s/v1/file/get_file_list',
                        {
                            'parentId': str(completed_root_id),
                            'page': page,
                            'pageSize': page_size,
                            **self.LIST_PAGE_PARAMS,
                        },
                        self.credential_provider._api_headers(ctx.access_token),
                    )
                    page_items = self._items(data)
                    if not page_items:
                        break
                    items.extend(page_items)
                    if not self._has_more(data, page=page, page_size=page_size, item_count=len(page_items)):
                        break
                else:
                    return {'status': 'UNVERIFIED', 'conflict': True, 'error': 'DIRECT_CHILD_PAGINATION_LIMIT'}
            return CompletedRootConflictScanner.inspect_direct_children(
                items,
                tmdb_id=int(tmdb_id),
                title=title,
            )
        except Exception as exc:  # noqa: BLE001 - fail closed for promotion
            return {'status': 'UNVERIFIED', 'conflict': True, 'error': type(exc).__name__}

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
            if operation != 'promote' and resume_stage in {'RESTORED', 'RESTORE_VERIFIED', 'RENAMING'}:
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
                if (prefix := self._layout_prefix(payload)):
                    payload['remote_rel_path_prefix'] = prefix
                return TransferOutcome(
                    True,
                    True,
                    remote_folder_id=resume_target,
                    remote_files=self._record_names(final_records),
                    remote_file_records=tuple(self._record_payload(final_records)),
                    rename_status=rename_status,
                )

            if operation == 'promote':
                source_series_id = str(payload.get('promotion_source_series_folder_id') or '').strip()
                if not source_series_id:
                    raise PromotionUnverifiedError('promotion source series folder ID is required')
                promotion_stage = str(payload.get('promotion_stage') or '').upper()
                if promotion_stage == 'MOVED':
                    series_id = str(payload.get('promotion_series_folder_id') or '').strip()
                    season_id = str(payload.get('promotion_season_folder_id') or '').strip()
                    if not series_id:
                        raise PromotionUnverifiedError('promotion readback fence has no series folder ID')
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
                except PromotionUnverifiedError:
                    payload['promotion_stage'] = 'MOVED'
                    payload['promotion_series_folder_id'] = series_id
                    payload['promotion_season_folder_id'] = season_id
                    payload['promotion_readback'] = {
                        'status': 'PROMOTION_UNVERIFIED',
                        'observed_by_season': observed_by_season if 'observed_by_season' in locals() else {},
                    }
                    raise
                payload['promotion_status'] = 'PROMOTION_COMPLETED'
                payload['promotion_readback'] = {
                    'status': 'PROMOTION_COMPLETED',
                    'observed_by_season': observed_by_season,
                }
                payload['remote_rel_path_prefix'] = str(payload.get('series_folder_name') or '').strip()
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
            source_id_by_name = {
                str(item.get('name') or item.get('fileName') or '').strip(): str(item.get('fileId') or item.get('id') or '').strip()
                for item in selected
                if str(item.get('name') or item.get('fileName') or '').strip()
            }
            for item in selected_target_records:
                if not self._item_id(item):
                    source_id = source_id_by_name.get(str(item.get('name') or item.get('fileName') or '').strip())
                    if source_id:
                        selected_target_ids.add(source_id)
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
            )
