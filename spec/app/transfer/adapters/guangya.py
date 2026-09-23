import asyncio
import json
from urllib.parse import parse_qs, urlparse

import httpx

from app.core.exceptions import TransferNotAllowed
from app.transfer.adapters import BaseAdapter
from app.transfer.status import TransferOutcome


class GuangyaAdapter(BaseAdapter):
    """Isolated Guangya adapter: paginated selection, async restore, verified directory readback."""

    provider = 'guangya'
    api_base = 'https://api.guangyapan.com'
    account_base = 'https://account.guangyapan.com'
    client_id = 'aMe-8VSlkrbQXpUR'

    @staticmethod
    def parse_auth_tokens(raw: str) -> tuple[str | None, str | None]:
        raw = (raw or '').strip()
        if not raw:
            return None, None
        if raw.startswith('{'):
            try:
                data = json.loads(raw)
                return data.get('access_token') or data.get('token'), data.get('refresh_token') or data.get('refreshToken')
            except json.JSONDecodeError:
                return None, None
        parts = raw.split()
        return (None, raw) if len(parts) == 1 and raw.startswith('gy.') else ((parts[0], parts[1]) if len(parts) > 1 else (raw, None))

    @staticmethod
    def share_parts(url: str) -> tuple[str, str]:
        parsed = urlparse(url if '://' in url else f'https://{url}')
        share_id = parsed.path.rstrip('/').split('/')[-1]
        code = (parse_qs(parsed.query).get('code') or [''])[0]
        return share_id, code

    @staticmethod
    def response_ok(data: dict) -> bool:
        return str(data.get('code', '')).strip() in {'0', '200'} or data.get('msg', '').lower() in {'', 'ok', 'success'}

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

    async def post(self, client: httpx.AsyncClient, url: str, payload: dict, headers: dict) -> dict:
        response = await client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or not self.response_ok(data):
            code = data.get('code') if isinstance(data, dict) else 'invalid-response'
            raise RuntimeError(f'guangya API rejected request: {code}')
        return data

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
        for page in range(1, max_pages + 1):
            data = await self.post(client, url, {**payload, page_key: page, 'page': page, 'pageSize': page_size}, headers)
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

    async def _list_share_files_recursive(
        self,
        client: httpx.AsyncClient,
        *,
        access_token: str,
        headers: dict,
        page_size: int = 100,
        max_pages: int = 1000,
        max_depth: int = 4,
    ) -> list[dict]:
        collected_files: list[dict] = []
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
                if item.get('resType') == 2:
                    if depth < max_depth:
                        fid = str(item.get('fileId') or item.get('id') or '')
                        if fid:
                            dir_queue.append((fid, depth + 1))
                else:
                    collected_files.append(item)

        return collected_files

    async def _readback_until_verified(
        self,
        client: httpx.AsyncClient,
        *,
        target_id: str,
        headers: dict,
        expected: set[str],
        attempts: int,
        interval_seconds: float,
    ) -> tuple[bool, set[str]]:
        observed: set[str] = set()
        for attempt in range(attempts):
            items = await self._list_all_pages(
                client,
                url=f'{self.api_base}/userres/v1/file/get_file_list',
                payload={'parentId': target_id},
                headers=headers,
                page_key='pageNum',
                page_size=200,
                max_pages=1000,
            )
            observed = {str(item.get('name') or item.get('fileName')) for item in items if item.get('resType') != 2}
            if not expected or expected.issubset(observed):
                return True, observed
            if attempt + 1 < attempts:
                await asyncio.sleep(interval_seconds)
        return False, observed

    @staticmethod
    def _item_id(item: dict) -> str:
        return str(item.get('fileId') or item.get('id') or '').strip()

    async def _list_folder_items(self, client: httpx.AsyncClient, *, parent_id: str, headers: dict) -> list[dict]:
        return await self._list_all_pages(
            client,
            url=f'{self.api_base}/nd.bizuserres.s/v1/file/get_file_list',
            payload={'parentId': parent_id},
            headers=headers,
            page_key='pageNum',
            page_size=200,
            max_pages=1000,
        )

    async def _ensure_directory(
        self,
        client: httpx.AsyncClient,
        *,
        parent_id: str,
        name: str,
        headers: dict,
    ) -> str:
        """Return one exact child directory, creating it only when absent."""
        existing = [
            item for item in await self._list_folder_items(client, parent_id=parent_id, headers=headers)
            if item.get('resType') == 2 and str(item.get('name') or item.get('fileName') or '') == name
        ]
        if len(existing) > 1:
            raise RuntimeError(f'ambiguous remote directory {name!r} below {parent_id}')
        if existing:
            folder_id = self._item_id(existing[0])
            if folder_id:
                return folder_id
            raise RuntimeError(f'remote directory {name!r} has no file ID')
        await self.post(
            client,
            f'{self.api_base}/nd.bizuserres.s/v1/file/create_dir',
            {'dirName': name, 'parentId': parent_id},
            headers,
        )
        created = [
            item for item in await self._list_folder_items(client, parent_id=parent_id, headers=headers)
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
        headers: dict,
    ) -> tuple[str, str]:
        """Create season layout or move one verified ongoing series into its final root."""
        series_name = str(payload.get('series_folder_name') or '').strip()
        season_name = str(payload.get('season_folder_name') or '').strip()
        if not series_name or not season_name:
            return root_id, root_id

        promotion_source_id = str(payload.get('promotion_source_series_folder_id') or '').strip()
        if promotion_source_id:
            root_items = await self._list_folder_items(client, parent_id=root_id, headers=headers)
            collisions = [
                item for item in root_items
                if item.get('resType') == 2 and str(item.get('name') or item.get('fileName') or '') == series_name
                and self._item_id(item) != promotion_source_id
            ]
            if collisions:
                raise RuntimeError(f'completed destination already contains series directory {series_name!r}')
            await self.post(
                client,
                f'{self.api_base}/nd.bizuserres.s/v1/file/move_file',
                {'fileIds': [promotion_source_id], 'parentId': root_id},
                headers,
            )
            root_items = await self._list_folder_items(client, parent_id=root_id, headers=headers)
            moved = [item for item in root_items if self._item_id(item) == promotion_source_id and item.get('resType') == 2]
            if len(moved) != 1:
                raise RuntimeError('could not verify moved ongoing series directory in completed root')
            current_name = str(moved[0].get('name') or moved[0].get('fileName') or '')
            if current_name != series_name:
                await self.post(
                    client,
                    f'{self.api_base}/nd.bizuserres.s/v1/file/rename',
                    {'fileId': promotion_source_id, 'newName': series_name},
                    headers,
                )
                root_items = await self._list_folder_items(client, parent_id=root_id, headers=headers)
                renamed = [
                    item for item in root_items
                    if self._item_id(item) == promotion_source_id
                    and str(item.get('name') or item.get('fileName') or '') == series_name
                ]
                if len(renamed) != 1:
                    raise RuntimeError('could not verify completed series directory rename')
            series_id = promotion_source_id
        else:
            series_id = await self._ensure_directory(client, parent_id=root_id, name=series_name, headers=headers)
        season_id = await self._ensure_directory(client, parent_id=series_id, name=season_name, headers=headers)
        return series_id, season_id

    async def refresh_access_token(self, client: httpx.AsyncClient, refresh_token: str) -> str | None:
        data = await self.post(
            client,
            f'{self.account_base}/v1/auth/token',
            {'client_id': self.client_id, 'grant_type': 'refresh_token', 'refresh_token': refresh_token},
            {
                'Content-Type': 'application/json',
                'Origin': 'https://account.guangyapan.com',
                'Referer': 'https://account.guangyapan.com/',
            },
        )
        payload = data.get('data') if isinstance(data.get('data'), dict) else {}
        return data.get('access_token') or payload.get('access_token') or payload.get('accessToken')

    async def list_directories(self, *, auth_token: str, parent_id: str) -> list[dict]:
        """Read direct child directories only; this method never restores or moves data."""
        auth = str(auth_token or '').strip()
        root_id = str(parent_id or '').strip()
        if not auth or not root_id:
            raise ValueError('auth_token and parent_id are required for directory inspection')
        access, refresh = self.parse_auth_tokens(auth)
        timeout = httpx.Timeout(30)
        async with httpx.AsyncClient(timeout=timeout) as client:
            if not access and refresh:
                access = await self.refresh_access_token(client, refresh)
            if not access:
                raise RuntimeError('access token is missing or refresh failed')
            headers = {
                'Content-Type': 'application/json',
                'Origin': 'https://www.guangyapan.com',
                'Referer': 'https://www.guangyapan.com/',
                'Authorization': f'Bearer {access}',
            }
            items = await self._list_folder_items(client, parent_id=root_id, headers=headers)
        return [item for item in items if item.get('resType') == 2]

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
        access, refresh = self.parse_auth_tokens(auth)
        timeout = httpx.Timeout(float(payload.get('timeout_seconds', 30)))
        page_size = int(payload.get('page_size', 100))
        max_pages = int(payload.get('max_pages', 1000))
        async with httpx.AsyncClient(timeout=timeout) as client:
            if not access and refresh:
                access = await self.refresh_access_token(client, refresh)
            if not access:
                return TransferOutcome(False, False, error='access token is missing or refresh failed')
            headers = {
                'Content-Type': 'application/json',
                'Origin': 'https://www.guangyapan.com',
                'Referer': 'https://www.guangyapan.com/',
                'Authorization': f'Bearer {access}',
            }
            destination_headers = headers
            if operation == 'promote':
                series_id, season_id = await self._prepare_destination_layout(
                    client,
                    payload=payload,
                    root_id=target_id,
                    headers=destination_headers,
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
                {key: value for key, value in headers.items() if key != 'Authorization'},
            )
            access_token = (token_data.get('data') or {}).get('accessToken')
            if not access_token:
                return TransferOutcome(False, False, error='share access token missing')
            items = await self._list_share_files_recursive(
                client,
                access_token=access_token,
                headers={key: value for key, value in headers.items() if key != 'Authorization'},
                page_size=page_size,
                max_pages=max_pages,
            )
            expected = {str(item) for item in payload.get('expected_files') or []}
            selected = [
                item for item in items
                if item.get('resType') != 2 and (not expected or str(item.get('name') or item.get('fileName')) in expected)
            ]
            selected_names = {str(item.get('name') or item.get('fileName')) for item in selected}
            if expected and selected_names != expected:
                return TransferOutcome(False, False, error='share listing did not contain every expected file')
            ids = list(dict.fromkeys(str(item.get('fileId') or item.get('id')) for item in selected if item.get('fileId') or item.get('id')))
            if not ids:
                return TransferOutcome(False, False, error='no video files selected from share')
            series_id, transfer_target_id = await self._prepare_destination_layout(
                client,
                payload=payload,
                root_id=target_id,
                headers=destination_headers,
            )
            await self.post(
                client,
                f'{self.api_base}/nd.bizuserres.s/v1/restore_share',
                {'accessToken': access_token, 'fileIds': ids, 'parentId': transfer_target_id},
                headers,
            )
            verified, observed = await self._readback_until_verified(
                client,
                target_id=transfer_target_id,
                headers=headers,
                expected=expected,
                attempts=max(1, int(payload.get('verify_attempts', 5))),
                interval_seconds=max(0, float(payload.get('verify_interval_seconds', 2))),
            )
            return TransferOutcome(
                verified,
                verified,
                remote_folder_id=transfer_target_id,
                remote_files=tuple(sorted(observed)),
                error=None if verified else 'remote readback missing expected files',
                remote_series_folder_id=series_id,
                remote_destination_kind=str(payload.get('destination_kind') or '') or None,
            )
