from loguru import logger
import os

# FIX: Use Airflow logs directory inside Docker
LOGS_DIR = os.getenv("LOGS_DIR", "/opt/airflow/logs")

os.makedirs(LOGS_DIR, exist_ok=True)

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


def log_exception(error: Exception) -> None:
    logger.exception(f"Exception occurred: {error}")