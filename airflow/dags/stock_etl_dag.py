from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import os

# List of stock symbols
SYMBOLS = ["AAPL", "MSFT", "GOOG", "TSLA"]


def failure_callback(context):
    """
    Send a Slack notification when a task fails.
    Only sends if SLACK_WEBHOOK_URL is set; otherwise logs a warning.
    """
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_webhook_url:
        from src.utils.logger import logger
        logger.warning("SLACK_WEBHOOK_URL not set; skipping Slack alert")
        return

    # Use SlackWebhookOperator to send message
    # We create an operator and call execute directly (this is acceptable for callbacks)
    slack = SlackWebhookOperator(
        task_id="slack_failure",
        http_conn_id="slack_webhook",
        webhook_token=slack_webhook_url,
        message=f"Task {context['task_instance'].task_id} failed in DAG {context['dag'].dag_id}",
        # Optional: include more context like log link
    )
    slack.execute(context=context)


# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback,
}


def create_etl_task(symbol: str):
    def run_etl():
        from src.main import main
        main(symbol=symbol)

    return PythonOperator(
        task_id=f"etl_{symbol}",
        python_callable=run_etl,
        execution_timeout=timedelta(minutes=10),
    )


with DAG(
    dag_id="stock_market_etl",
    default_args=default_args,
    description="ETL pipeline for multiple stock symbols",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "stocks"],
    doc_md="""
    # Stock Market ETL Pipeline

    This DAG extracts stock data for multiple symbols, transforms it,
    and loads it into PostgreSQL.

    ## Features:
    - Parallel execution per stock symbol
    - Retry logic
    - Scalable architecture
    - Slack alerts on failure (requires SLACK_WEBHOOK_URL env variable)

    ## Dependencies:
    - apache-airflow-providers-slack
    """,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    etl_tasks = [create_etl_task(symbol) for symbol in SYMBOLS]

    start >> etl_tasks >> end