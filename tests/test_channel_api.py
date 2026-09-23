import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.app import app
from app.core.database import Base, get_db


@pytest.mark.asyncio
async def test_channel_api_configures_manual_ingest_and_uses_it_for_forward(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/api.db')
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def override_get_db():
        async with sessions() as session:
            yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    app.dependency_overrides[get_db] = override_get_db
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url='http://testserver') as client:
            configured = await client.post('/channels', json={
                'channel_id': '-700', 'role': 'MANUAL_INGEST', 'enabled': True,
                'accept_forward': True, 'transfer_mode': 'AUTO',
            })
            assert configured.status_code == 200
            assert configured.json()['role'] == 'MANUAL_INGEST'
            ingested = await client.post('/ingest/source-message', json={
                'source_type': 'manual_forward', 'channel_id': '-700', 'message_id': 9,
                'text': '已知资源 S01E01 https://pan.guangyapan.com/s/demo', 'is_forward': True,
                'metadata': {'tmdb_id': 700, 'title': '已知资源', 'file_names': ['已知资源.S01E01.mkv']},
            })
            assert ingested.status_code == 200
            assert ingested.json()['queued'] is True
            assert ingested.json()['is_forward'] is True
    finally:
        app.dependency_overrides.pop(get_db, None)
        await engine.dispose()
