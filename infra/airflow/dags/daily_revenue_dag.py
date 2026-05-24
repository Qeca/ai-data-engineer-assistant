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
    'daily_revenue_dag',
    default_args=default_args,
    description='Daily revenue aggregation from sales data',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'revenue', 'daily'],
)

spark_job = SparkSubmitOperator(
    task_id='run_daily_revenue_spark',
    application='infra/spark/jobs/daily_revenue.py',
    name='daily_revenue_aggregation',
    executor_memory='2g',
    total_executor_cores=2,
    conf={
        'spark.sql.shuffle.partitions': '4',
    },
    dag=dag,
)

spark_job
