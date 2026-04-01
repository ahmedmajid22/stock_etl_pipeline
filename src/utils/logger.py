import os
import sys
from pathlib import Path
from loguru import logger

# Resolve log directory based on environment
if os.getenv("AIRFLOW_HOME"):
    LOGS_DIR = Path(os.getenv("AIRFLOW_HOME")) / "logs"
elif os.path.exists("/opt/airflow"):
    LOGS_DIR = Path("/opt/airflow/logs")
else:
    LOGS_DIR = Path(__file__).resolve().parents[2] / "logs"

LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Remove default Loguru handler
logger.remove()

# Rotating file handler — daily rotation, 7-day retention
logger.add(
    str(LOGS_DIR / "app.log"),
    rotation="00:00",
    retention="7 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name} | {message}",
    enqueue=True,       # thread-safe for Celery workers
    backtrace=True,     # full traceback on exceptions
    diagnose=True,      # variable values in tracebacks
)

# Stdout handler — visible in Airflow task logs UI
logger.add(
    sys.stdout,
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name} | {message}",
    enqueue=True,
)