import asyncio
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.database import AsyncSessionLocal
from app.models.channel import ChannelSetting
from app.models.cloud import CloudConfig

logger = logging.getLogger(__name__)

DEFAULT_CHANNELS = [
    {
        'channel_id': '-1004429917555',
        'channel_name': '光鸭云盘资源频道',
        'role': 'RESOURCE',
        'transfer_mode': 'AUTO',
        'accept_forward': False,
        'enabled': True,
        'default_provider': 'guangya',
    },
    {
        'channel_id': '-1003808659413',
        'channel_name': '光鸭云盘影视热更频道',
        'role': 'RESOURCE',
        'transfer_mode': 'AUTO',
        'accept_forward': False,
        'enabled': True,
        'default_provider': 'guangya',
    },
    {
        'channel_id': '-1003974900477',
        'channel_name': '剧开心（百度夸克迅雷光鸭移动123网盘分享）',
        'role': 'RESOURCE',
        'transfer_mode': 'AUTO',
        'accept_forward': False,
        'enabled': True,
        'default_provider': 'guangya',
    },
    {
        'channel_id': '-1004435637826',
        'channel_name': '光鸭云盘资源收藏',
        'role': 'RESOURCE',
        'transfer_mode': 'AUTO',
        'accept_forward': False,
        'enabled': True,
        'default_provider': 'guangya',
    },
    {
        'channel_id': '-1003667471790',
        'channel_name': '光鸭云盘资源频道',
        'role': 'RESOURCE',
        'transfer_mode': 'AUTO',
        'accept_forward': False,
        'enabled': True,
        'default_provider': 'guangya',
    },
    {
        'channel_id': '-1003702243011',
        'channel_name': '光鸭云盘资源分享群',
        'role': 'RESOURCE',
        'transfer_mode': 'AUTO',
        'accept_forward': False,
        'enabled': True,
        'default_provider': 'guangya',
    },
    {
        'channel_id': '-1003961136374',
        'channel_name': '测试频道',
        'role': 'MANUAL_INGEST',
        'transfer_mode': 'AUTO',
        'accept_forward': True,
        'enabled': True,
        'default_provider': 'guangya',
    },
    {
        'channel_id': '-1004387965244',
        'channel_name': '光鸭发布频道',
        'role': 'RESOURCE',
        'transfer_mode': 'OFF',
        'accept_forward': False,
        'enabled': True,
        'default_provider': 'guangya',
    },
]


async def import_cloud_configs(
    db: AsyncSession,
    cloud_items: list[dict[str, Any]],
) -> dict[str, int]:
    created = updated = 0
    for item in cloud_items:
        name = item['name']
        existing = await db.scalar(select(CloudConfig).where(CloudConfig.name == name))
        if existing:
            existing.domain_pattern = item.get('domain_pattern', existing.domain_pattern)
            existing.auth_ref = item.get('auth_ref', existing.auth_ref)
            existing.target_folder_id = item.get('target_folder_id', existing.target_folder_id)
            existing.ongoing_target_folder_id = item.get('ongoing_target_folder_id', existing.ongoing_target_folder_id)
            existing.channel_id = item.get('channel_id', existing.channel_id)
            existing.enabled = item.get('enabled', existing.enabled)
            updated += 1
        else:
            db.add(
                CloudConfig(
                    name=name,
                    domain_pattern=item.get('domain_pattern'),
                    auth_ref=item.get('auth_ref'),
                    target_folder_id=item.get('target_folder_id'),
                    ongoing_target_folder_id=item.get('ongoing_target_folder_id'),
                    channel_id=item.get('channel_id'),
                    enabled=item.get('enabled', True),
                )
            )
            created += 1
    return {'created': created, 'updated': updated}


async def import_channel_settings(
    db: AsyncSession,
    channel_items: list[dict[str, Any]],
) -> dict[str, int]:
    created = updated = 0
    for item in channel_items:
        cid = str(item['channel_id'])
        existing = await db.scalar(select(ChannelSetting).where(ChannelSetting.channel_id == cid))
        if existing:
            existing.channel_name = item.get('channel_name', existing.channel_name)
            existing.role = item.get('role', existing.role)
            existing.transfer_mode = item.get('transfer_mode', existing.transfer_mode)
            existing.accept_forward = item.get('accept_forward', existing.accept_forward)
            existing.enabled = item.get('enabled', existing.enabled)
            existing.default_provider = item.get('default_provider', existing.default_provider)
            existing.default_category = item.get('default_category', existing.default_category)
            updated += 1
        else:
            db.add(
                ChannelSetting(
                    channel_id=cid,
                    channel_name=item.get('channel_name'),
                    role=item.get('role', 'RESOURCE'),
                    transfer_mode=item.get('transfer_mode', 'AUTO'),
                    accept_forward=item.get('accept_forward', False),
                    enabled=item.get('enabled', True),
                    default_provider=item.get('default_provider', 'guangya'),
                    default_category=item.get('default_category'),
                )
            )
            created += 1
    return {'created': created, 'updated': updated}


async def run_import(
    *,
    clouds: list[dict[str, Any]] | None = None,
    channels: list[dict[str, Any]] | None = None,
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionLocal,
    report_path: str | None = None,
) -> dict[str, Any]:
    cloud_items = clouds or []
    channel_items = channels if channels is not None else DEFAULT_CHANNELS

    async with session_factory() as db, db.begin():
        cloud_res = await import_cloud_configs(db, cloud_items)
        channel_res = await import_channel_settings(db, channel_items)

    report = {
        'timestamp': datetime.now(UTC).isoformat(),
        'clouds': cloud_res,
        'channels': channel_res,
        'cloud_names': [c['name'] for c in cloud_items],
        'channel_ids': [c['channel_id'] for c in channel_items],
    }

    if report_path:
        p = Path(report_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

    return report


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--clouds-json', help='Path to clouds JSON file')
    parser.add_argument('--channels-json', help='Path to channels JSON file')
    parser.add_argument('--report', help='Report output path')
    args = parser.parse_args()

    c_list = json.loads(Path(args.clouds_json).read_text()) if args.clouds_json else None
    ch_list = json.loads(Path(args.channels_json).read_text()) if args.channels_json else None

    res = asyncio.run(run_import(clouds=c_list, channels=ch_list, report_path=args.report))
    print(json.dumps(res, ensure_ascii=False))
