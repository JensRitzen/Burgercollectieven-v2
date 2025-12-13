import os
import sys
import time
import io
import zipfile
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

import requests
import pandas as pd

print(" QUALTRICS POLLER GESTART")

# =========================
# Config uit environment file
# =========================
QUALTRICS_API_TOKEN = os.getenv("QUALTRICS_API_TOKEN")
QUALTRICS_DATACENTER = os.getenv("QUALTRICS_DATA_CENTER")
SURVEY_ID = os.getenv("QUALTRICS_SURVEY_ID")

DB_PATH = "data/qualtrics.db"
LOG_DIR = "logs"
POLL_INTERVAL_SECONDS = 60

# Request timeouts om mogelijke freezes tegen te gaan
TIMEOUT_EXPORT_START = 30
TIMEOUT_STATUS_CHECK = 30
TIMEOUT_FILE_DOWNLOAD = 60


# =========================
# Logging setup toegevoegd na contact stakeholder
# =========================
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("qualtrics_poller")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (docker logs)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # File handler (persistent)
    fh = RotatingFileHandler(
        os.path.join(LOG_DIR, "poller.log"),
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


logger = setup_logging()


# =========================
# 1) DB initialisatie
# =========================
def initialize_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # created_at / updated_at toegevoegd omdat ik mutaties wil kunnen zien
    c.execute("""
        CREATE TABLE IF NOT EXISTS responses (
            ResponseId TEXT PRIMARY KEY,
            data TEXT,
            created_at TEXT,
            updated_at TEXT
        );
    """)

   
    # Kolommen created_at en updated_at toevoegen.
    c.execute("PRAGMA table_info(responses);")
    existing_cols = {row[1] for row in c.fetchall()}  # row[1] = kolomnaam

    if "created_at" not in existing_cols:
        c.execute("ALTER TABLE responses ADD COLUMN created_at TEXT;")
        logger.info("[DB] Migratie: kolom created_at toegevoegd.")

    if "updated_at" not in existing_cols:
        c.execute("ALTER TABLE responses ADD COLUMN updated_at TEXT;")
        logger.info("[DB] Migratie: kolom updated_at toegevoegd.")
    

    conn.commit()
    conn.close()
    logger.info("[DB] Database geïnitialiseerd / gecontroleerd.")


# =========================
# 2) UPSERT maakt mutaties op bestaande response ID mogelijk
# =========================
def upsert_row(row: pd.Series):
    response_id = row.get("ResponseId")
    if pd.isna(response_id):
        return

    response_id = str(response_id)
    json_data = row.to_json()
    now = datetime.utcnow().isoformat()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # INSERT als nieuw, UPDATE als ResponseId al bestaat
    c.execute("""
        INSERT INTO responses (ResponseId, data, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ResponseId) DO UPDATE SET
            data = excluded.data,
            created_at = COALESCE(responses.created_at, excluded.created_at),
            updated_at = excluded.updated_at;
    """, (response_id, json_data, now, now))

    conn.commit()
    conn.close()


def count_rows() -> int:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM responses")
    count = c.fetchone()[0]
    conn.close()
    return int(count)


# =========================
# 3) Downloaden & verwerken
# =========================
def download_responses():
    logger.info("[INFO] Polling cycle gestart...")

    if not QUALTRICS_API_TOKEN or not QUALTRICS_DATACENTER or not SURVEY_ID:
        logger.error("[CONFIG] Missing env vars. Check Tokens.env (QUALTRICS_API_TOKEN / DATA_CENTER / SURVEY_ID).")
        return

    headers = {
        "X-API-TOKEN": QUALTRICS_API_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/octet-stream",
    }

    # Start export
    start_url = f"https://{QUALTRICS_DATACENTER}.qualtrics.com/API/v3/surveys/{SURVEY_ID}/export-responses/"
    logger.info(f"[INFO] Start export: {start_url}")

    try:
        start_resp = requests.post(
            start_url,
            headers=headers,
            json={"format": "csv"},
            timeout=TIMEOUT_EXPORT_START,
        )
        logger.info(f"[DEBUG] Export init status: {start_resp.status_code}")
    except requests.RequestException:
        logger.exception("[ERROR] Export init request faalde (network/timeout).")
        return

    if start_resp.status_code != 200:
        logger.error(f"[ERROR] Export starten mislukt: {start_resp.text}")
        return

    progress_id = start_resp.json()["result"]["progressId"]
    logger.info(f"[INFO] Export gestart met progressId: {progress_id}")

    # Wacht tot export klaar is
    file_id = None
    start_wait = time.time()

    while True:
        status_url = f"{start_url}{progress_id}"
        try:
            status_resp = requests.get(status_url, headers=headers, timeout=TIMEOUT_STATUS_CHECK)
        except requests.RequestException:
            logger.exception("[ERROR] Status check faalde (network/timeout).")
            return

        if status_resp.status_code != 200:
            logger.error(f"[ERROR] Status check failed: {status_resp.status_code} - {status_resp.text}")
            return

        result = status_resp.json()["result"]
        percent = result.get("percentComplete", 0)
        logger.info(f"[INFO] Export voortgang: {percent}%")

        if percent == 100:
            file_id = result["fileId"]
            logger.info(f"[INFO] Export gereed, fileId: {file_id}")
            break

        # Als export langer duurt dan 10 min stopt deze cycle
        if time.time() - start_wait > 600:
            logger.error("[ERROR] Export duurt langer dan 10 minuten. Cycle afgebroken.")
            return

        time.sleep(2)

    # Download file
    file_url = f"{start_url}{file_id}/file"
    logger.info(f"[INFO] Downloaden: {file_url}")

    try:
        file_resp = requests.get(file_url, headers=headers, timeout=TIMEOUT_FILE_DOWNLOAD)
    except requests.RequestException:
        logger.exception("[ERROR] Download request faalde (network/timeout).")
        return

    if file_resp.status_code != 200:
        logger.error(f"[ERROR] Download mislukt: {file_resp.status_code} - {file_resp.text}")
        return

    # ZIP uitpakken
    try:
        z = zipfile.ZipFile(io.BytesIO(file_resp.content))
        z.extractall("data")
    except Exception:
        logger.exception("[ERROR] ZIP uitpakken faalde.")
        return

    # Vind CSV
    csv_files = [f for f in z.namelist() if f.endswith(".csv")]
    if not csv_files:
        logger.error("[ERROR] Geen CSV gevonden in ZIP.")
        return

    csv_file = csv_files[0]
    logger.info(f"[INFO] CSV gevonden: {csv_file}")

    csv_path = os.path.join("data", csv_file)

    # CSV inlezen
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        logger.exception(f"[ERROR] CSV inlezen faalde: {csv_path}")
        return

    logger.info(f"[INFO] CSV geladen: {df.shape[0]} rijen x {df.shape[1]} kolommen")

    # Opslaan (UPSERT)
    logger.info("[INFO] Opslaan in SQLite (insert/update)...")

    count_before = count_rows()

    
    try:
        for _, row in df.iterrows():
            upsert_row(row)
    except Exception:
        logger.exception("[ERROR] Schrijven naar SQLite faalde tijdens upsert.")
        return

    count_after = count_rows()
    logger.info(f"[SUCCESS] DB records totaal: {count_after} | wijziging (kan 0 zijn bij alleen updates): {count_after - count_before}")


# =========================
# Start poller
# =========================
if __name__ == "__main__":
    initialize_db()
    logger.info("Qualtrics poller gestart")

    while True:
        try:
            download_responses()
        except Exception:
            # Silent failure logging
            logger.exception("[CRITICAL] Onverwachte crash in polling cycle.")

        logger.info(f"Wachten tot volgende polling ({POLL_INTERVAL_SECONDS} sec)...")
        time.sleep(POLL_INTERVAL_SECONDS)
