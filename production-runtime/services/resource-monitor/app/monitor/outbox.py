import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path


class ResourceOutbox:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _init(self):
        with sqlite3.connect(self.path) as db:
            db.execute('''CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT NOT NULL, chat_title TEXT,
                message_id INTEGER NOT NULL, text TEXT NOT NULL, urls TEXT NOT NULL DEFAULT '[]',
                source_type TEXT NOT NULL, is_forward INTEGER NOT NULL DEFAULT 0, date TEXT,
                UNIQUE(chat_id, message_id))''')
            db.execute('''CREATE TABLE IF NOT EXISTS resource_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT, channel_id TEXT NOT NULL, message_id INTEGER NOT NULL,
                payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING', attempt_count INTEGER NOT NULL DEFAULT 0,
                next_retry_at TEXT, last_error TEXT, created_at TEXT NOT NULL, sent_at TEXT,
                UNIQUE(channel_id, message_id))''')

    def enqueue(self, payload: dict) -> bool:
        with sqlite3.connect(self.path) as db:
            cur=db.execute('''INSERT OR IGNORE INTO messages(chat_id,chat_title,message_id,text,urls,source_type,is_forward,date)
                              VALUES(?,?,?,?,?,?,?,?)''',(payload['channel_id'],payload.get('channel_title'),payload['message_id'],payload.get('text') or payload.get('caption') or '',json.dumps(payload.get('urls') or [],ensure_ascii=False),payload.get('source_type','telegram_channel'),int(bool(payload.get('is_forward'))),payload.get('published_at')))
            db.execute('''INSERT OR IGNORE INTO resource_outbox(channel_id,message_id,payload,status,created_at)
                              VALUES(?,?,?,?,?)''',(payload['channel_id'],payload['message_id'],json.dumps(payload,ensure_ascii=False),'PENDING',datetime.now(UTC).isoformat()))
            return cur.rowcount == 1

    def pending(self, limit: int = 50) -> list[dict]:
        with sqlite3.connect(self.path) as db:
            rows=db.execute("SELECT id,payload FROM resource_outbox WHERE status IN ('PENDING','RETRY') ORDER BY id LIMIT ?",(limit,)).fetchall()
        return [{'id': row[0], **json.loads(row[1])} for row in rows]

    def mark_sent(self, row_id: int) -> None:
        with sqlite3.connect(self.path) as db: db.execute("UPDATE resource_outbox SET status='SENT',sent_at=? WHERE id=?",(datetime.now(UTC).isoformat(),row_id))

    def mark_retry(self, row_id: int, error: str) -> None:
        with sqlite3.connect(self.path) as db: db.execute("UPDATE resource_outbox SET status='RETRY',attempt_count=attempt_count+1,last_error=? WHERE id=?",(error[:1000],row_id))
