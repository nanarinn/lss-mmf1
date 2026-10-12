from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def connect(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute('CREATE TABLE IF NOT EXISTS readings (topic TEXT PRIMARY KEY, received_at TEXT NOT NULL, record TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
    db.commit()
    return db


def save_batch(path, records, metadata):
    db = connect(path)
    try:
        with db:
            for record in records:
                received = datetime.fromisoformat(record['received_at_utc']).astimezone(timezone.utc).isoformat()
                record = dict(record, received_at_utc=received)
                db.execute('INSERT INTO readings VALUES (?, ?, ?) ON CONFLICT(topic) DO UPDATE SET received_at=excluded.received_at, record=excluded.record WHERE excluded.received_at > readings.received_at',
                           (record['topic'], received, json.dumps(record, ensure_ascii=False)))
            # Reject out-of-order refresh metadata too (eg a delayed bridge upload).
            old = db.execute("SELECT value FROM metadata WHERE key='last_refresh'").fetchone()
            old_time = json.loads(old[0]).get('completed_at_utc', '') if old else ''
            if metadata['completed_at_utc'] > old_time:
                db.execute("INSERT OR REPLACE INTO metadata VALUES ('last_refresh', ?)", (json.dumps(metadata),))
    finally:
        db.close()


def load(path):
    db = connect(path)
    try:
        # One read transaction gives a consistent view during a refresh.
        db.execute('BEGIN')
        rows = [json.loads(row[0]) for row in db.execute('SELECT record FROM readings ORDER BY topic')]
        meta = db.execute("SELECT value FROM metadata WHERE key='last_refresh'").fetchone()
        return rows, json.loads(meta[0]) if meta else None
    finally:
        db.close()
