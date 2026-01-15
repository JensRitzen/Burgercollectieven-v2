import time
import sys
import subprocess

from loggers.logger import Logger

from poller.qualtrics_client import QualtricsClient
from poller.export_service import ExportService
from poller.csv_handler import CsvHandler
from poller.database import Database
from poller.poller import QualtricsPoller

from config import POLL_INTERVAL_SECONDS

VISUALS_SCRIPT_PATH = "/app/CollectieveKracht_VisualsScript.py"

def run_visuals(logger):
    """
    Draait het visuals script na de poller.
    """
    try:
        result = subprocess.run(
            [sys.executable, VISUALS_SCRIPT_PATH],
            capture_output=True,
            text=True,
            check=True,
        )

        if result.stdout.strip():
            logger.info(f"[visuals stdout]\n{result.stdout}")

        if result.stderr.strip():
            logger.warning(f"[visuals stderr]\n{result.stderr}")

        logger.info("Visuals-script succesvol afgerond.")

    except FileNotFoundError:
        logger.exception(
            f"Visuals-script niet gevonden op {VISUALS_SCRIPT_PATH}. "
            "Controleer of het bestand is meegekopieerd in de Docker image."
        )
    except subprocess.CalledProcessError as e:
        logger.error("Visuals-script faalde.")
        if e.stdout:
            logger.error(f"[visuals stdout]\n{e.stdout}")
        if e.stderr:
            logger.error(f"[visuals stderr]\n{e.stderr}")
    except Exception:
        logger.exception("Onverwachte fout bij het draaien van het visuals-script.")

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
