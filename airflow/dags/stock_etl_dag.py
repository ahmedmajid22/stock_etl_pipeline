from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Import your ETL main function
from src.main import main


# Default arguments (production best practice)
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# List of stock symbols
SYMBOLS = ["AAPL", "MSFT", "GOOG", "TSLA"]


def create_etl_task(symbol: str):
    """
    Factory function to create an ETL task for a given stock symbol.
    """

    return PythonOperator(
        task_id=f"etl_{symbol}",
        python_callable=main,
        op_kwargs={"symbol": symbol},
        execution_timeout=timedelta(minutes=10),
    )


# DAG definition
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
    """,
    # DAG-level timeout
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # Create tasks
    etl_tasks = [create_etl_task(symbol) for symbol in SYMBOLS]

    # Define dependencies: start -> all tasks -> end
    start >> etl_tasks >> end