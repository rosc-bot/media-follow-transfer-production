import sqlite3
from pathlib import Path


def inspect_ingest_history(path: str) -> dict:
    uri=f'file:{Path(path).resolve()}?mode=ro'
    with sqlite3.connect(uri, uri=True) as db:
        tables={r[0] for r in db.execute("select name from sqlite_master where type='table'")}
        result={}
        for name in ('auto_ingest_history','failed_scout_pushes','notified_ingest_jobs'):
            result[name]=db.execute(f'select count(*) from "{name}"').fetchone()[0] if name in tables else 0
        return result
