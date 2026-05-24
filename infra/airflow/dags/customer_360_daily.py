from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'customer_360_daily',
    default_args=default_args,
    description='Daily Spark job to join customers, orders, and events into customer_360.parquet',
    schedule_interval='0 3 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'customer_360', 'daily'],
)

spark_join_task = SparkSubmitOperator(
    task_id='run_customer_360_spark',
    application='/workspace/infra/spark/jobs/customer_360_join.py',
    name='customer_360_join',
    executor_memory='2g',
    total_executor_cores=2,
    conf={
        'spark.sql.shuffle.partitions': '4',
    },
    dag=dag,
)

spark_join_task