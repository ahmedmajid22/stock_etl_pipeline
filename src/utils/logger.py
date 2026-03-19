# src/utils/logger.py

from loguru import logger
import os
from datetime import datetime

# Ensure logs directory exists
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)

# Configure logger
logger.add(
    os.path.join(LOGS_DIR, "app.log"),
    rotation="00:00",  # Daily rotation at midnight
    retention="7 days",  # Keep last 7 days of logs
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    enqueue=True,
    backtrace=True,
    diagnose=True
)

def log_exception(error: Exception) -> None:
    """
    Log an exception with full stack trace.

    Args:
        error (Exception): Exception object to log
    """
    logger.exception(f"Exception occurred: {error}")