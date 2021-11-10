import logging
import os
from logging.handlers import TimedRotatingFileHandler

os.makedirs("logs", exist_ok=True)

log_formatter = logging.Formatter("%(asctime)s [%(levelname)-8s] %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(logging.INFO)

file_handler = TimedRotatingFileHandler(
    "logs/sczone-crawler", when="midnight", interval=1, backupCount=365, encoding="utf-8"
)
file_handler.suffix = "%Y%m%d.log"
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)


def debug(region_no, msg):
    logger.debug(f"({region_no}) {msg}")


def info(region_no, msg):
    logger.info(f"({region_no}) {msg}")


def warn(region_no, msg):
    logger.warn(f"({region_no}) {msg}")


def error(region_no, msg):
    logger.error(f"({region_no}) {msg}")
