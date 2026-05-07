from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task():
    print('Hello from my_task!')

with DAG(
    dag_id='agent_smoke_dag',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=my_task,
    )
