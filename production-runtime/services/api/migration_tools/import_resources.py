import sqlite3
from pathlib import Path


def inspect_resource_messages(path: str) -> dict:
    uri=f'file:{Path(path).resolve()}?mode=ro'
    with sqlite3.connect(uri, uri=True) as db:
        tables={r[0] for r in db.execute("select name from sqlite_master where type='table'")}
        if 'messages' not in tables: return {'messages':0,'source_exists':False}
        count=db.execute('select count(*) from messages').fetchone()[0]
        return {'messages':count,'source_exists':True}
