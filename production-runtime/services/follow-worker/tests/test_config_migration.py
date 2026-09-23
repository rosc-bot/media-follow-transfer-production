import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.channel import ChannelSetting
from app.models.cloud import CloudConfig
from migration_tools.import_configs import run_import


@pytest.mark.asyncio
async def test_import_configs_is_idempotent(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path}/test.db')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    test_clouds = [
        {
            'name': 'guangya',
            'domain_pattern': 'guangya|gypan',
            'auth_ref': '{"token": "test"}',
            'target_folder_id': '12345',
            'channel_id': '-1004387965244',
            'enabled': True,
        }
    ]

    report_file = tmp_path / 'report.json'

    # Run 1: initial creation
    res1 = await run_import(
        clouds=test_clouds,
        session_factory=sessions,
        report_path=str(report_file),
    )
    assert res1['clouds']['created'] == 1
    assert res1['clouds']['updated'] == 0
    assert res1['channels']['created'] == 8
    assert res1['channels']['updated'] == 0

    async with sessions() as db:
        gy = await db.scalar(select(CloudConfig).where(CloudConfig.name == 'guangya'))
        assert gy is not None
        assert gy.target_folder_id == '12345'
        assert gy.channel_id == '-1004387965244'

        channels = list((await db.scalars(select(ChannelSetting))).all())
        assert len(channels) == 8

    # Run 2: idempotent re-run
    test_clouds[0]['target_folder_id'] = '67890'
    res2 = await run_import(
        clouds=test_clouds,
        session_factory=sessions,
        report_path=str(report_file),
    )
    assert res2['clouds']['created'] == 0
    assert res2['clouds']['updated'] == 1
    assert res2['channels']['created'] == 0
    assert res2['channels']['updated'] == 8

    async with sessions() as db:
        gy = await db.scalar(select(CloudConfig).where(CloudConfig.name == 'guangya'))
        assert gy.target_folder_id == '67890'
