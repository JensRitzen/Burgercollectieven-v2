import time


class ExportService:
    def __init__(self, client, logger):
        self.client = client
        self.logger = logger

    def run_export(self) -> bytes:
        progress_id = self.client.start_export()
        self.logger.info(f"Export gestart: {progress_id}")

        start_time = time.time()

        while True:
            status = self.client.check_status(progress_id)
            percent = status.get("percentComplete", 0)

            self.logger.info(f"Export voortgang: {percent}%")

            if percent == 100:
                return self.client.download_file(status["fileId"])

            if time.time() - start_time > 600:
                raise TimeoutError("Export duurde langer dan 10 minuten")

            time.sleep(2)
