import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.database import Base
from app.follow.legacy_root_discovery_service import LegacyRootDiscoveryService
from app.models.cloud import CloudConfig
from app.models.resource import Resource
from app.models.watchlist import SeriesWatchlist


@pytest.mark.asyncio
async def test_legacy_discovery_backfills_only_one_exact_tmdb_marked_ongoing_root(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/legacy-discovery.db')
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        db.add(CloudConfig(
            name='guangya', auth_ref='test-auth', enabled=True,
            target_folder_id='completed-root', ongoing_target_folder_id='ongoing-root',
        ))
        db.add_all([
            SeriesWatchlist(tmdb_id=12, title='可回填', season=1, status='FOLLOWING'),
            SeriesWatchlist(tmdb_id=34, title='歧义拒绝', season=1, status='FOLLOWING'),
        ])
        db.add_all([
            Resource(id=12, identity_key='legacy-resource-12', tmdb_id=12, title='可回填', season=1,
                     cloud_name='guangya', share_url='https://pan.guangyapan.com/s/12', source_type='watchlist_scout'),
            Resource(id=34, identity_key='legacy-resource-34', tmdb_id=34, title='歧义拒绝', season=1,
                     cloud_name='guangya', share_url='https://pan.guangyapan.com/s/34', source_type='watchlist_scout'),
        ])

    async def list_directories(*, provider, auth_token, parent_id):
        assert (provider, auth_token, parent_id) == ('guangya', 'test-auth', 'ongoing-root')
        return [
            {'fileId': 'folder-12', 'name': '可回填 (2025) {tmdbid-12}', 'resType': 2},
            {'fileId': 'folder-34a', 'name': '歧义拒绝 A {tmdbid-34}', 'resType': 2},
            {'fileId': 'folder-34b', 'name': '歧义拒绝 B {tmdbid-34}', 'resType': 2},
            {'fileId': 'folder-120', 'name': '不应误匹配 {tmdbid-120}', 'resType': 2},
        ]

    async with sessions() as db, db.begin():
        discovered = await LegacyRootDiscoveryService(list_directories).backfill(db)
        assert discovered == 1

    async with sessions() as db:
        found = await db.get(SeriesWatchlist, 1)
        ambiguous = await db.get(SeriesWatchlist, 2)
        assert found.remote_series_folder_id == 'folder-12'
        assert found.remote_destination_kind == 'ongoing'
        assert ambiguous.remote_series_folder_id is None
    await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_discovery_treats_provider_timeout_as_no_backfill(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/legacy-timeout.db')
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        db.add(CloudConfig(
            name='guangya', auth_ref='test-auth', enabled=True,
            target_folder_id='completed-root', ongoing_target_folder_id='ongoing-root',
        ))
        db.add(SeriesWatchlist(tmdb_id=12, title='超时剧', season=1, status='FOLLOWING'))
        db.add(Resource(
            id=12, identity_key='timeout-resource', tmdb_id=12, title='超时剧', season=1,
            cloud_name='guangya', share_url='https://pan.guangyapan.com/s/12', source_type='watchlist_scout',
        ))

    async def timeout_lister(**_kwargs):
        raise httpx.ConnectTimeout('provider unavailable')

    async with sessions() as db, db.begin():
        assert await LegacyRootDiscoveryService(timeout_lister).backfill(db) == 0
    await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_discovery_treats_provider_auth_rejection_as_no_backfill(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/legacy-auth.db')
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        db.add(CloudConfig(
            name='guangya', auth_ref='test-auth', enabled=True,
            target_folder_id='completed-root', ongoing_target_folder_id='ongoing-root',
        ))
        db.add(SeriesWatchlist(tmdb_id=13, title='鉴权剧', season=1, status='FOLLOWING'))
        db.add(Resource(
            id=13, identity_key='auth-resource', tmdb_id=13, title='鉴权剧', season=1,
            cloud_name='guangya', share_url='https://pan.guangyapan.com/s/13', source_type='watchlist_scout',
        ))

    request = httpx.Request('POST', 'https://api.guangyapan.com/nd.bizuserres.s/v1/file/get_file_list')

    async def rejected_lister(**_kwargs):
        raise httpx.HTTPStatusError('unauthorized', request=request, response=httpx.Response(401, request=request))

    async with sessions() as db, db.begin():
        assert await LegacyRootDiscoveryService(rejected_lister).backfill(db) == 0
    await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_discovery_auth_expired_never_blocks_follow(tmp_path):
    """Item 17: GuangyaAuthExpiredError from the lister must surface as a structured
    skip (0 backfills) without escaping into the follow cycle."""
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.database import Base
    from app.models.cloud import CloudConfig
    from app.models.resource import Resource
    from app.transfer.errors import GuangyaAuthExpiredError

    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/legacy-auth-expired.db')
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as db, db.begin():
        db.add(CloudConfig(
            name='guangya', auth_ref='test-auth', enabled=True,
            target_folder_id='completed-root', ongoing_target_folder_id='ongoing-root',
        ))
        from app.follow.legacy_root_discovery_service import LegacyRootDiscoveryService
        from app.models.watchlist import SeriesWatchlist

        db.add(SeriesWatchlist(tmdb_id=4242, title='认证剧', season=1, status='FOLLOWING'))
        db.add(Resource(
            id=4242, identity_key='auth-expired-res', tmdb_id=4242, title='认证剧', season=1,
            cloud_name='guangya', share_url='https://pan.guangyapan.com/s/4242', source_type='watchlist_scout',
        ))

    async def auth_expired_lister(**_kwargs):
        raise GuangyaAuthExpiredError('access token expired')

    async with sessions() as db, db.begin():
        discovered = await LegacyRootDiscoveryService(auth_expired_lister).backfill(db)
        assert discovered == 0  # no exception escapes -> follow cycle continues
    await engine.dispose()
