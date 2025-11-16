print(" QUALTRICS POLLER GESTART")

import requests
import time
import zipfile
import io
import pandas as pd
import sqlite3
import os

QUALTRICS_API_TOKEN = os.getenv("QUALTRICS_API_TOKEN")
QUALTRICS_DATACENTER = os.getenv("QUALTRICS_DATA_CENTER")
SURVEY_ID = os.getenv("QUALTRICS_SURVEY_ID")


# 1. Zorg dat de database klaarstaat met PK

def initialize_db():
    conn = sqlite3.connect("data/qualtrics.db")
    c = conn.cursor()

    # Maak tabel als die nog niet bestaat
    c.execute("""
        CREATE TABLE IF NOT EXISTS responses (
            ResponseId TEXT PRIMARY KEY,
            data JSON
        );
    """)

    conn.commit()
    conn.close()



# 2. Upsert functie om dubbele data te voorkomen

def insert_row(row):
    conn = sqlite3.connect("data/qualtrics.db")
    c = conn.cursor()

   
    response_id = row["ResponseId"]
    json_data = row.to_json()

    c.execute("""
        INSERT OR IGNORE INTO responses (ResponseId, data)
        VALUES (?, ?);
    """, (response_id, json_data))

    conn.commit()
    conn.close()



# 3. Downloaden & verwerken

def download_responses():
    print(" [INFO] Polling gestart...")

    headers = {
        "X-API-TOKEN": QUALTRICS_API_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/octet-stream"
    }

    # Start export
    start_url = f"https://{QUALTRICS_DATACENTER}.qualtrics.com/API/v3/surveys/{SURVEY_ID}/export-responses/"
    print(f"  [INFO] Start export: {start_url}")

    start_resp = requests.post(start_url, headers=headers, json={"format": "csv"})
    print(f" [DEBUG] Export init status: {start_resp.status_code}")

    if start_resp.status_code != 200:
        print(" [ERROR] Export starten mislukt:", start_resp.text)
        return

    progress_id = start_resp.json()["result"]["progressId"]
    print(f" [INFO] Export gestart met progressId: {progress_id}")

    # Wacht tot export klaar is
    while True:
        status_url = f"{start_url}{progress_id}"
        status_resp = requests.get(status_url, headers=headers)
        result = status_resp.json()["result"]

        percent = result["percentComplete"]
        print(f" [INFO] Export voortgang: {percent}%")

        if percent == 100:
            file_id = result["fileId"]
            print(f" [INFO] Export gereed, fileId: {file_id}")
            break

        time.sleep(2)

    # Download file
    file_url = f"{start_url}{file_id}/file"
    print(f"⬇️  [INFO] Downloaden: {file_url}")

    file_resp = requests.get(file_url, headers=headers)
    z = zipfile.ZipFile(io.BytesIO(file_resp.content))
    z.extractall("data")

    # Vind CSV
    csv_file = [f for f in z.namelist() if f.endswith(".csv")][0]
    print(f" [INFO] CSV gevonden: {csv_file}")

    df = pd.read_csv(os.path.join("data", csv_file))

    
    # DEDUPLICATIE
    
    print(" [INFO] Opslaan zonder duplicaten...")

    count_before = count_rows()

    for _, row in df.iterrows():
        insert_row(row)

    count_after = count_rows()

    print(f" [SUCCESS] Nieuwe records toegevoegd: {count_after - count_before}\n")


def count_rows():
    conn = sqlite3.connect("data/qualtrics.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM responses")
    count = c.fetchone()[0]
    conn.close()
    return count


# Start poller

if __name__ == "__main__":
    initialize_db()
    print("Qualtrics poller gestart")

    while True:
        download_responses()
        print(" Wachten tot volgende polling (60 sec)...\n")
        time.sleep(60)
