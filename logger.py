import logging
import sys
import os
from logging.handlers import RotatingFileHandler
from config import LOG_DIR


class Logger:
    @staticmethod
    def create_logger(name: str) -> logging.Logger:
        os.makedirs(LOG_DIR, exist_ok=True)

        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        if logger.handlers:
            return logger

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)

        file_handler = RotatingFileHandler(
            os.path.join(LOG_DIR, "poller.log"),
            maxBytes=5_000_000,
            backupCount=5,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)

        return logger
