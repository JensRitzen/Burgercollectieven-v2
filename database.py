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

    def _migrate_add_missing_columns(self, conn: sqlite3.Connection) -> None:
        c = conn.cursor()
        c.execute("PRAGMA table_info(responses)")
        existing = {row[1] for row in c.fetchall()}  

        def add_col(sql: str, col_name: str):
            if col_name not in existing:
                self.logger.info(f"DB migratie: kolom toevoegen -> {col_name}")
                c.execute(sql)

        add_col("ALTER TABLE responses ADD COLUMN scan_status TEXT DEFAULT 'NEW'", "scan_status")
        add_col("ALTER TABLE responses ADD COLUMN scanned_at TEXT", "scanned_at")
        add_col("ALTER TABLE responses ADD COLUMN scan_error TEXT", "scan_error")

    def upsert(self, response_id: str, data: str):
        now = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        
        c.execute(
            """
            INSERT INTO responses (ResponseId, data, created_at, updated_at, scan_status)
            VALUES (?, ?, ?, ?, 'NEW')
            ON CONFLICT(ResponseId) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at,
                scan_status = CASE
                    WHEN responses.data != excluded.data THEN 'NEW'
                    ELSE responses.scan_status
                END
            """,
            (response_id, data, now, now),
        )

        conn.commit()
        conn.close()

    
    def fetch_unscanned(self, limit: int = 500) -> List[Tuple[str, str]]:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            """
            SELECT ResponseId, data
            FROM responses
            WHERE COALESCE(scan_status, 'NEW') = 'NEW'
            ORDER BY updated_at ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = c.fetchall()
        conn.close()
        return rows

    
    def mark_scanned(self, response_id: str, status: str = "DONE", error: Optional[str] = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            """
            UPDATE responses
            SET scan_status = ?, scanned_at = ?, scan_error = ?
            WHERE ResponseId = ?
            """,
            (status, now, error, response_id),
        )
        conn.commit()
        conn.close()

    def count(self) -> int:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM responses")
        count = c.fetchone()[0]
        conn.close()
        return count

   
