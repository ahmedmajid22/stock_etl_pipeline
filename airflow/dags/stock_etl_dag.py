from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.main import main

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="stock_market_etl",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["etl", "stocks"],
) as dag:

    run_etl_task = PythonOperator(
        task_id="execute_etl_pipeline",
        python_callable=main,
        op_kwargs={"symbol": "AAPL"},
    )