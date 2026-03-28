from loguru import logger
import os
import sys

# FIX: Use Airflow logs directory inside Docker
LOGS_DIR = os.getenv("LOGS_DIR", "/opt/airflow/logs")

os.makedirs(LOGS_DIR, exist_ok=True)

# File logger (rotating)
logger.add(
    os.path.join(LOGS_DIR, "app.log"),
    rotation="00:00",
    retention="7 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    enqueue=True,
    backtrace=True,
    diagnose=True
)

# Console logger (for Airflow UI)
logger.add(
    sys.stdout,
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    enqueue=True
)


def log_exception(error: Exception) -> None:
    logger.exception(f"Exception occurred: {error}")