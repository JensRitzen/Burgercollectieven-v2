import requests
from config import (
    QUALTRICS_API_TOKEN,
    QUALTRICS_DATACENTER,
    SURVEY_ID,
    TIMEOUT_EXPORT_START,
    TIMEOUT_STATUS_CHECK,
    TIMEOUT_FILE_DOWNLOAD
)


class QualtricsClient:
    def __init__(self, logger):
        self.logger = logger
        self.base_url = (
            f"https://{QUALTRICS_DATACENTER}.qualtrics.com/API/v3/"
            f"surveys/{SURVEY_ID}/export-responses/"
        )
        self.headers = {
            "X-API-TOKEN": QUALTRICS_API_TOKEN,
            "Content-Type": "application/json",
            "Accept": "application/octet-stream"
        }

    def start_export(self) -> str:
        response = requests.post(
            self.base_url,
            headers=self.headers,
            json={"format": "csv"},
            timeout=TIMEOUT_EXPORT_START
        )
        response.raise_for_status()
        return response.json()["result"]["progressId"]

    def check_status(self, progress_id: str) -> dict:
        response = requests.get(
            f"{self.base_url}{progress_id}",
            headers=self.headers,
            timeout=TIMEOUT_STATUS_CHECK
        )
        response.raise_for_status()
        return response.json()["result"]

    def download_file(self, file_id: str) -> bytes:
        response = requests.get(
            f"{self.base_url}{file_id}/file",
            headers=self.headers,
            timeout=TIMEOUT_FILE_DOWNLOAD
        )
        response.raise_for_status()
        return response.content
