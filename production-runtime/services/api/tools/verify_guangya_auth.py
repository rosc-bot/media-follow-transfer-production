"""Controlled read-only Guangya authentication verification (Phase 2A).

Executes exactly two GET-only listings against the configured Guangya account:
  1. first list_directories  - if the stored access token is stale this exercises
     401 -> single refresh -> retry -> 200, and persists the refreshed token.
  2. second list_directories - proves the refreshed token was persisted and is
     used directly (no second refresh needed).

It NEVER restores, moves, renames, deletes, creates folders or writes media.
The whole run is intentionally limited to the read-only get_file_list calls.

Report fields: initial_status, refresh_occurred, refresh_status, retry_status,
credential_persisted, directory_read_success, second_call_status.
No token contents are ever printed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.errors import GuangyaTransferError
from app.transfer.guangya_auth import (
    GuangyaCredentialProvider,
    GuangyaCredentialStore,
    parse_auth_ref,
)


class ProbingProvider(GuangyaCredentialProvider):
    """Counts refresh attempts without changing behavior."""

    def __init__(self) -> None:
        super().__init__()
        self.refresh_calls = 0
        self.last_refresh_failure: str | None = None

    async def refresh_access(self, client: httpx.AsyncClient, refresh_token: str) -> tuple[str, str | None]:
        self.refresh_calls += 1
        try:
            return await super().refresh_access(client, refresh_token)
        except GuangyaTransferError as exc:
            self.last_refresh_failure = str(exc.category)
            raise


class ProbingAdapter(GuangyaAdapter):
    """Counts 401s seen by the transport without changing behavior."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.unauthorized_count = 0

    async def post(self, client: httpx.AsyncClient, url: str, payload: dict, headers: dict) -> dict:
        try:
            return await super().post(client, url, payload, headers)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 401:
                self.unauthorized_count += 1
            raise


async def verify(database_url: str, *, provider_name: str = 'guangya') -> dict:
    engine = create_async_engine(database_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    report: dict = {
        'provider': provider_name,
        'initial_status': 'UNKNOWN',
        'refresh_occurred': False,
        'refresh_status': 'not-needed',
        'retry_status': 'not-applicable',
        'credential_persisted': False,
        'directory_read_success': False,
        'second_call_status': 'skipped',
        'error': None,
    }
    try:
        async with sessions() as db:
            from app.models.cloud import CloudConfig

            cfg = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider_name))
            if cfg is None or not cfg.auth_ref:
                report['initial_status'] = 'NOT_CONFIGURED'
                return report
            root_id = str(cfg.ongoing_target_folder_id or cfg.target_folder_id or '').strip()
            if not root_id:
                report['initial_status'] = 'NO_TARGET_ROOT'
                return report
            before = parse_auth_ref(cfg.auth_ref)
            report['initial_status'] = (
                'HAS_ACCESS_AND_REFRESH'
                if before.get('access_token') and before.get('refresh_token')
                else 'PARTIAL_CREDENTIALS'
            )

        provider = ProbingProvider()
        store = GuangyaCredentialStore(sessions)
        adapter = ProbingAdapter(write_enabled=False, credential_provider=provider, credential_store=store)

        async with sessions() as db:
            cfg = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider_name))
            if cfg is None or not cfg.auth_ref:
                report['initial_status'] = 'NOT_CONFIGURED'
                return report
            auth_token = str(cfg.auth_ref or '')

        # First read-only listing: exercises 401 -> refresh -> retry -> 200 when stale.
        first_error: str | None = None
        try:
            folders = await adapter.list_directories(auth_token=auth_token, parent_id=root_id)
            report['refresh_occurred'] = provider.refresh_calls > 0
            if provider.refresh_calls:
                report['refresh_status'] = 'success'
                report['retry_status'] = 'OK'
            else:
                report['retry_status'] = 'no-401-seen'
            report['directory_read_success'] = True
            report['directory_count'] = len(folders)
        except (GuangyaTransferError, httpx.HTTPError) as exc:
            first_error = f'{type(exc).__name__}: {exc}'
            if provider.refresh_calls:
                report['refresh_status'] = f'failure:{provider.last_refresh_failure or "unknown"}'
                report['retry_status'] = 'AUTH_EXPIRED'
            else:
                report['retry_status'] = 'FAILED'

        if report['directory_read_success']:
            # Prove persistence: re-read the credential from the database and
            # issue a second listing with the *stored* token. It must succeed
            # with zero refreshes and zero 401s — proof the refreshed (or still
            # valid) credential was persisted and is used directly.
            try:
                async with sessions() as db:
                    cfg2 = await db.scalar(select(CloudConfig).where(CloudConfig.name == provider_name))
                    stored_token = str(cfg2.auth_ref or '') if cfg2 else ''
                unauthorized_before = adapter.unauthorized_count
                refresh_before = provider.refresh_calls
                await adapter.list_directories(auth_token=stored_token, parent_id=root_id)
                second_refresh = provider.refresh_calls - refresh_before
                second_401 = adapter.unauthorized_count - unauthorized_before
                if second_refresh == 0 and second_401 == 0:
                    report['second_call_status'] = 'OK_DIRECT_NO_REFRESH'
                    report['credential_persisted'] = True
                else:
                    report['second_call_status'] = f'OK_BUT_REFRESHED_AGAIN(refresh={second_refresh},401={second_401})'
            except (GuangyaTransferError, httpx.HTTPError) as exc:
                report['second_call_status'] = f'FAILED: {type(exc).__name__}'
        else:
            report['error'] = first_error
        return report
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--database-url', required=True)
    parser.add_argument('--output', default='-')
    args = parser.parse_args()
    report = asyncio.run(verify(args.database_url))
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + '\n'
    if args.output == '-':
        print(rendered, end='')
    else:
        Path(args.output).write_text(rendered, encoding='utf-8')
        print(json.dumps({'output': args.output, 'status': report['initial_status']}, ensure_ascii=False))
    if not report.get('directory_read_success'):
        sys.exit(2)


if __name__ == '__main__':
    main()
