import time
from loggers.logger import Logger
from poller.qualtrics_client import QualtricsClient
from poller.export_service import ExportService
from poller.csv_handler import CsvHandler
from poller.database import Database
from poller.poller import QualtricsPoller
from config import POLL_INTERVAL_SECONDS


def main():
    logger = Logger.create_logger("qualtrics_poller")

    client = QualtricsClient(logger)
    export_service = ExportService(client, logger)
    csv_handler = CsvHandler(logger)
    database = Database(logger)
    database.initialize()

    poller = QualtricsPoller(
        export_service=export_service,
        csv_handler=csv_handler,
        database=database,
        logger=logger
    )

    logger.info("Qualtrics poller gestart")

    while True:
        try:
            poller.run_once()
        except Exception:
            logger.exception("Onverwachte fout in polling cycle")

        logger.info(f"Wachten {POLL_INTERVAL_SECONDS} seconden")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
