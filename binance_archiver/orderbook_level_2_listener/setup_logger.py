import logging
import time
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os


def setup_logger(log_file_path: str | None = None) -> logging.Logger:
    logger = logging.getLogger('DaemonManager')
    logger.setLevel(logging.DEBUG)

    now_utc = datetime.utcnow().strftime('%d-%m-%YT%H-%M-%SZ')

    if log_file_path:
        if not os.path.exists(log_file_path):
            os.makedirs(log_file_path)

        file_handler = RotatingFileHandler(
            f"{log_file_path}/archiver_{now_utc}.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=3
        )

        logging.Formatter.converter = time.gmtime

        file_formatter = logging.Formatter('%(asctime)sZ - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)sZ - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    logger.raiseExceptions = True
    logger.logThreads = True
    logger.logMultiprocessing = True
    logger.logProcesses = True

    logging.raiseExceptions = True
    logging.logThreads = True
    logging.logMultiprocessing = True
    logging.logProcesses = True

    return logger
