import logging
import logging.config
import os
from datetime import datetime

# Base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Logs directory
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Get today's date
today_date = datetime.now().strftime("%d-%b-%Y")

main_log_file = os.path.join(LOG_DIR, f"main_{today_date}.log")
error_log_file = os.path.join(LOG_DIR, f"error_{today_date}.log")

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s"
        }
    },
    "handlers": {
        "console_handler": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file_handler": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "filename": main_log_file,
            "encoding": "utf-8",
        },
        "error_file_handler": {
            "class": "logging.FileHandler",
            "level": "ERROR",
            "formatter": "standard",
            "filename": error_log_file,
            "encoding": "utf-8",
        },
    },
    "root": {
        "level": "DEBUG",
        "handlers": [
            "console_handler",
            "file_handler",
            "error_file_handler",
        ],
    },
}

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger(__name__)
