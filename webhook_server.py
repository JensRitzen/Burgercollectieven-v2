from flask import Flask, request, jsonify
import sqlite3
import requests
import pandas as pd
import os

app = Flask(__name__)

QUALTRICS_API_TOKEN = os.getenv("QUALTRICS_API_TOKEN")
QUALTRICS_DATACENTER = os.getenv("QUALTRICS_DATA_CENTER")
SURVEY_ID = os.getenv("QUALTRICS_SURVEY_ID")

@app.route("/webhook", methods=["POST"])
def receive_webhook():
    payload = request.json
    result = payload.get("Result", {})
    response_id = result.get("ResponseID")

    print(f"Nieuwe response ontvangen: {response_id}")

    # Download response data via Qualtrics API
    headers = {"X-API-TOKEN": QUALTRICS_API_TOKEN}
    url = f"https://{QUALTRICS_DATACENTER}.qualtrics.com/API/v3/surveys/{SURVEY_ID}/responses/{response_id}"
    resp = requests.get(url, headers=headers)
    response_json = resp.json()

    # Transformeer naar DataFrame
    df = pd.json_normalize(response_json["result"])

    # Sla op in SQLite
    conn = sqlite3.connect("data/qualtrics.db")
    df.to_sql("responses", conn, if_exists="append", index=False)
    conn.close()

    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
