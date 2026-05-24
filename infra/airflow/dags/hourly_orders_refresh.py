from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    'hourly_orders_refresh',
    default_args=default_args,
    description='Hourly refresh of analytics.hourly_orders from sales.orders',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analytics', 'hourly'],
)

refresh_hourly_orders = PostgresOperator(
    task_id='refresh_hourly_orders',
    postgres_conn_id='demo-postgres-warehouse',
    sql="""
        INSERT INTO analytics.hourly_orders
        SELECT * FROM sales.orders
        WHERE ts >= now() - INTERVAL '1 hour'
    """,
    dag=dag,
)