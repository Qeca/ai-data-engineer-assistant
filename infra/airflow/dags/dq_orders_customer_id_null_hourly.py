from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dq_orders_customer_id_null_hourly',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags=['data_quality', 'spark', 'orders'],
)

run_spark_dq = BashOperator(
    task_id='run_spark_dq_check',
    bash_command='spark-submit /workspace/infra/spark/jobs/dq_orders_customer_id_null.py',
    dag=dag,
)

run_spark_dq