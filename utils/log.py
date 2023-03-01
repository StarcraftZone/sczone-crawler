import logging
import os
from logging.handlers import TimedRotatingFileHandler

os.makedirs("logs", exist_ok=True)

log_formatter = logging.Formatter("%(asctime)s [%(levelname)-8s] [%(threadName)-9s] %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(logging.INFO)

file_handler = TimedRotatingFileHandler("logs/sczone-crawler", when="midnight", interval=1, backupCount=365, encoding="utf-8")
file_handler.suffix = "%Y%m%d.log"
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)


def debug(region_no: int | None, msg):
    if region_no is None:
        logger.debug(f"{msg}")
    else:
        logger.debug(f"({region_no}) {msg}")


def info(region_no: int | None, msg):
    if region_no is None:
        logger.info(f"{msg}")
    else:
        logger.info(f"({region_no}) {msg}")


def warn(region_no: int | None, msg):
    if region_no is None:
        logger.warn(f"{msg}")
    else:
        logger.warn(f"({region_no}) {msg}")


def error(region_no: int | None, msg):
    if region_no is None:
        logger.error(f"{msg}")
    else:
        logger.error(f"({region_no}) {msg}")
