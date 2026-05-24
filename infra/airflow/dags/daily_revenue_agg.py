from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_revenue_agg',
    default_args=default_args,
    description='Ежедневная агрегация выручки по дням через Spark',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'revenue', 'daily'],
)

BashOperator(
    task_id='run_spark',
    bash_command='python /workspace/spark/daily_revenue.py',
    dag=dag,
)
