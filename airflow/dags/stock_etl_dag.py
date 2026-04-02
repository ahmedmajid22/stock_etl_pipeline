import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

SYMBOLS = ["AAPL", "MSFT", "GOOG", "TSLA"]


def failure_callback(context):
    """
    Send a Slack alert on task failure.
    Reads webhook URL from the 'slack_webhook' Airflow Connection (host field).
    Silently skips if the connection is not configured.
    """
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection("slack_webhook")
        webhook_url = conn.host
        if not webhook_url:
            return
    except Exception:
        return

    try:
        import requests

        ti = context["task_instance"]
        requests.post(
            webhook_url,
            json={
                "text": (
                    f":red_circle: *Task failed*\n"
                    f"DAG: `{context['dag'].dag_id}`\n"
                    f"Task: `{ti.task_id}`\n"
                    f"Run: `{context['run_id']}`\n"
                    f"Log: {ti.log_url}"
                )
            },
            timeout=5,
        )
    except Exception as e:
        from src.utils.logger import logger

        logger.warning(f"Slack alert failed: {e}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback,
}


# ─────────────────────────────────────────────
# Task callables
# ─────────────────────────────────────────────


def extract_task(symbol: str, **context):
    from src.config.config import Config
    from src.extract.api_client import AlphaVantageClient
    from src.storage.staging import write_raw_stage
    from src.utils.logger import logger

    cfg = Config()
    client = AlphaVantageClient(api_key=cfg.API_KEY)
    run_id = str(uuid.uuid4())

    logger.info(f"EXTRACT start: {symbol}")
    raw_data = client.get_daily_stock_data(symbol)

    raw_path = write_raw_stage(raw_data, symbol, run_id)

    context["ti"].xcom_push(key=f"run_id_{symbol}", value=run_id)
    context["ti"].xcom_push(key=f"raw_path_{symbol}", value=str(raw_path))

    record_count = len(raw_data.get("Time Series (Daily)", {}))
    logger.info(f"EXTRACT complete: {symbol} → {record_count} records → {raw_path}")
    return f"Extracted {record_count} records for {symbol}"


def transform_task(symbol: str, **context):
    from src.storage.staging import read_raw_stage, write_stage
    from src.transform.transformer import StockDataTransformer
    from src.utils.logger import logger

    ti = context["ti"]
    raw_path = ti.xcom_pull(key=f"raw_path_{symbol}", task_ids=f"extract_{symbol}")
    run_id = ti.xcom_pull(key=f"run_id_{symbol}", task_ids=f"extract_{symbol}")

    raw_data = read_raw_stage(raw_path)
    df = StockDataTransformer().transform(raw_data, symbol=symbol)
    transformed_path = write_stage(df, symbol, run_id)

    ti.xcom_push(key=f"transformed_path_{symbol}", value=str(transformed_path))

    logger.info(
        f"TRANSFORM complete: {symbol} → {len(df)} records → {transformed_path}"
    )
    return f"Transformed {len(df)} records for {symbol}"


def validate_task(symbol: str, **context):
    import pandas as pd
    from src.storage.staging import write_stage
    from src.transform.validator import StockDataValidator
    from src.utils.logger import logger

    ti = context["ti"]
    path = ti.xcom_pull(
        key=f"transformed_path_{symbol}", task_ids=f"transform_{symbol}"
    )
    run_id = ti.xcom_pull(key=f"run_id_{symbol}", task_ids=f"extract_{symbol}")

    df = pd.read_parquet(path)
    df = StockDataValidator().validate(df)
    validated_path = write_stage(df, symbol, run_id)

    ti.xcom_push(key=f"validated_path_{symbol}", value=str(validated_path))

    logger.info(f"VALIDATE complete: {symbol} → {len(df)} records → {validated_path}")
    return f"Validated {len(df)} records for {symbol}"


def stage_task(symbol: str, **context):
    """Confirms staging is complete. Data is already on disk from validate_task."""
    from src.utils.logger import logger

    validated_path = context["ti"].xcom_pull(
        key=f"validated_path_{symbol}", task_ids=f"validate_{symbol}"
    )
    logger.info(f"STAGING confirmed: {symbol} → {validated_path}")
    return f"Staged at {validated_path}"


def load_task(symbol: str, **context):
    import pandas as pd
    from src.config.config import Config
    from src.load.database import DatabaseLoader
    from src.utils.logger import logger

    ti = context["ti"]
    path = ti.xcom_pull(key=f"validated_path_{symbol}", task_ids=f"validate_{symbol}")
    df = pd.read_parquet(path)

    cfg = Config()
    loader = DatabaseLoader(cfg.get_db_connection_string())

    logger.info(f"LOAD start: {symbol} → {len(df)} records")
    loader.upsert_dataframe(df, symbol)
    logger.info(f"LOAD complete: {symbol}")
    return f"Loaded {len(df)} records for {symbol}"


# ─────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────

with DAG(
    dag_id="stock_market_etl_v2",
    default_args=default_args,
    description="Production ETL — extract → transform → validate → stage → load",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "stocks", "production"],
    dagrun_timeout=timedelta(minutes=30),
    doc_md="""
# Stock Market ETL Pipeline

Ingests daily OHLCV data for AAPL, MSFT, GOOG, TSLA from Alpha Vantage
into PostgreSQL. Each symbol runs 5 tasks in parallel.

## Task graph per symbol
`extract → transform → validate → stage → load`

## Key design decisions
- XCom carries only file paths, never DataFrames
- Raw JSON and Parquet files persist in `data/` for replay
- UPSERT on (stock_id, date) — safe to re-run
- API calls capped at 3 concurrent via `alpha_vantage_pool`
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for symbol in SYMBOLS:
        extract = PythonOperator(
            task_id=f"extract_{symbol}",
            python_callable=extract_task,
            op_args=[symbol],
            execution_timeout=timedelta(minutes=5),
            pool="alpha_vantage_pool",  # cap concurrent API calls
            pool_slots=1,
        )
        transform = PythonOperator(
            task_id=f"transform_{symbol}",
            python_callable=transform_task,
            op_args=[symbol],
            execution_timeout=timedelta(minutes=5),
        )
        validate = PythonOperator(
            task_id=f"validate_{symbol}",
            python_callable=validate_task,
            op_args=[symbol],
            execution_timeout=timedelta(minutes=5),
        )
        stage = PythonOperator(
            task_id=f"stage_{symbol}",
            python_callable=stage_task,
            op_args=[symbol],
            execution_timeout=timedelta(minutes=2),
        )
        load = PythonOperator(
            task_id=f"load_{symbol}",
            python_callable=load_task,
            op_args=[symbol],
            execution_timeout=timedelta(minutes=10),
        )

        start >> extract >> transform >> validate >> stage >> load >> end
