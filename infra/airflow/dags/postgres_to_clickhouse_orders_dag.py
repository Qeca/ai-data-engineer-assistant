from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'postgres_to_clickhouse_orders_dag',
    default_args=default_args,
    description='Копирование данных из sales.orders (Postgres) в analytics.events (ClickHouse) с трансформацией',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'postgres', 'clickhouse', 'etl'],
)

spark_submit = SparkSubmitOperator(
    task_id='spark_postgres_to_clickhouse',
    application='/workspace/infra/spark/jobs/postgres_to_clickhouse_orders.py',
    name='postgres_to_clickhouse_orders',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.jars.packages': 'org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.6.0',
        'spark.executor.memory': '2g',
        'spark.driver.memory': '1g',
    },
    dag=dag,
)

spark_submit
