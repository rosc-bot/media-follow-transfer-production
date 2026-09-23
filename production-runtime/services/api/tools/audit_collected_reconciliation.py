"""Read-only legacy/new/cloud collected-episode reconciliation."""
from __future__ import annotations

import argparse
import asyncio
import json
from collections import defaultdict
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

try:
    from tools.audit_queue_safety import load_legacy_collected, normalize_episode_key
except ModuleNotFoundError:  # Direct script execution sets sys.path to tools/.
    from audit_queue_safety import load_legacy_collected, normalize_episode_key


async def reconcile(database_url: str, legacy_watchlist: str) -> dict:
    old = load_legacy_collected(legacy_watchlist)
    engine = create_async_engine(database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn, conn.begin():
                await conn.execute(text("SET TRANSACTION READ ONLY"))
                watches = (await conn.execute(text("SELECT tmdb_id,title,season,collected_episodes FROM series_watchlist"))).mappings().all()
                inventory = (await conn.execute(text("SELECT tmdb_id,season,episode FROM cloud_disk_inventory WHERE tmdb_id IS NOT NULL"))).mappings().all()
        new: dict[tuple[int, int], set[str]] = defaultdict(set)
        titles: dict[tuple[int, int], str] = {}
        for row in watches:
            key=(int(row['tmdb_id']),int(row['season']))
            titles[key]=row['title']
            values=row['collected_episodes'] if isinstance(row['collected_episodes'],list) else []
            for raw in values:
                ep=normalize_episode_key(row['season'],raw)
                if ep: new[key].add(ep)
        cloud={(int(r['tmdb_id']),int(r['season']),normalize_episode_key(r['season'],r['episode'])) for r in inventory}
        rows=[]
        all_identities=set(old)|set(new)
        for identity in sorted(all_identities):
            tmdb_id,season=identity
            for ep in sorted(old.get(identity,set()) ^ new.get(identity,set())):
                old_hit=ep in old.get(identity,set())
                new_hit=ep in new.get(identity,set())
                cloud_hit=(tmdb_id,season,ep) in cloud
                if old_hit and not new_hit:
                    verdict='VERIFIED_IN_CLOUD' if cloud_hit else 'OLD_DB_ONLY'
                elif new_hit and not old_hit:
                    verdict='VERIFIED_IN_CLOUD' if cloud_hit else 'NEW_DB_ONLY'
                else:
                    verdict='CONFLICT'
                rows.append({'title':titles.get(identity),'tmdb_id':tmdb_id,'season':season,'episode':ep,
                             'old_collected':old_hit,'new_collected':new_hit,'cloud_inventory':cloud_hit,'verdict':verdict})
        return {'read_only':True,'difference_count':len(rows),'summary':{k:sum(r['verdict']==k for r in rows) for k in sorted({r['verdict'] for r in rows})},'episodes':rows}
    finally:
        await engine.dispose()


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--database-url',required=True);p.add_argument('--legacy-watchlist',required=True);p.add_argument('--output',default='-')
    args=p.parse_args(); result=asyncio.run(reconcile(args.database_url,args.legacy_watchlist)); body=json.dumps(result,ensure_ascii=False,indent=2)
    if args.output=='-': print(body)
    else: Path(args.output).write_text(body); print(json.dumps({'output':args.output,'difference_count':result['difference_count'],'read_only':True}))
if __name__=='__main__': main()
