import pytest
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine

import app.models  # noqa: F401
from app.core.database import Base


@pytest.mark.asyncio
async def test_isolated_schema_contains_only_media_domains(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/schema.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        names = await conn.run_sync(lambda c: inspect(c).get_table_names())
    await engine.dispose()
    assert set(names) == {
        'admin_audit_log', 'app_settings', 'auto_ingest_history', 'bot_settings', 'channel_ingest_jobs',
        'channel_ingest_messages', 'channel_settings', 'cloud_configs', 'cloud_disk_inventory',
        'failed_scout_pushes', 'ignored_missing', 'resource_candidates', 'resources',
        'series_watchlist', 'telegram_admins', 'telegram_users', 'transfer_jobs', 'transfer_queue_tasks',
    }
