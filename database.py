import sqlite3
from datetime import datetime, timezone
from typing import Optional, List, Tuple
from config import DB_PATH


class Database:
    def __init__(self, logger):
        self.logger = logger

    def initialize(self):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS responses (
                ResponseId TEXT PRIMARY KEY,
                data TEXT,
                created_at TEXT,
                updated_at TEXT,
                scan_status TEXT DEFAULT 'NEW',
                scanned_at TEXT,
                scan_error TEXT
            );
        """)

        self._migrate_add_missing_columns(conn)

        conn.commit()
        conn.close()
        self.logger.info("Database geinitialiseerd")

    def upsert(self, response_id: str, data: str):
        now = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("""
            INSERT INTO responses (ResponseId, data, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ResponseId) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at;
        """, (response_id, data, now, now))

        conn.commit()
        conn.close()

    def count(self) -> int:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM responses")
        count = c.fetchone()[0]
        conn.close()
        return count
