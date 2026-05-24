from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_revenue_spark',
    default_args=default_args,
    description='Ежедневная агрегация выручки по дням через Spark',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'revenue', 'daily'],
)

run_spark_task = BashOperator(
    task_id='run_daily_revenue_spark',
    bash_command='python /workspace/spark/daily_revenue.py',
    dag=dag,
)

run_spark_task
