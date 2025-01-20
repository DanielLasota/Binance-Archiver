import logging
import sys
import time
from logging.handlers import RotatingFileHandler


def setup_logger(should_dump_logs: bool | None = False) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logging.Formatter.converter = time.gmtime

    console_formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03dZ %(levelname)s -- %(message)s",
        datefmt='%Y-%m-%dT%H:%M:%S'
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(console_formatter)

    logger.addHandler(stream_handler)

    if should_dump_logs is True:

        file_handler = RotatingFileHandler(
            filename="archiver.log",
            maxBytes=5*1024*1024,
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setFormatter(console_formatter)

        logger.addHandler(file_handler)

        def log_exception(exc_type, exc_value, exc_traceback):
            logger.error("Exception: ", exc_info=(exc_type, exc_value, exc_traceback))

        sys.excepthook = log_exception

    return logger
