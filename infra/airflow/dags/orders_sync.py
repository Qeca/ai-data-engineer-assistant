from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="orders_sync",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "data-team"},
) as dag:
    extract = EmptyOperator(task_id="extract_orders")
    validate = EmptyOperator(task_id="validate_orders")
    load = EmptyOperator(task_id="load_orders")

    extract >> validate >> load
