from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_orders_aggregation',
    default_args=default_args,
    description='Агрегация заказов за день из sales.orders в analytics.daily_orders',
    schedule_interval='@daily',
    catchup=False,
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS analytics.daily_orders (
    order_date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_amount NUMERIC(18,2),
    paid_orders INTEGER,
    paid_amount NUMERIC(18,2),
    cancelled_orders INTEGER,
    cancelled_amount NUMERIC(18,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

AGGREGATE_SQL = """
INSERT INTO analytics.daily_orders (
    order_date, total_orders, total_amount,
    paid_orders, paid_amount,
    cancelled_orders, cancelled_amount,
    updated_at
)
SELECT
    DATE(order_ts) AS order_date,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_amount,
    COUNT(*) FILTER (WHERE status = 'paid') AS paid_orders,
    COALESCE(SUM(amount) FILTER (WHERE status = 'paid'), 0) AS paid_amount,
    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled_orders,
    COALESCE(SUM(amount) FILTER (WHERE status = 'cancelled'), 0) AS cancelled_amount,
    CURRENT_TIMESTAMP AS updated_at
FROM sales.orders
WHERE DATE(order_ts) = '{{ ds }}'
GROUP BY DATE(order_ts)
ON CONFLICT (order_date) DO UPDATE SET
    total_orders = EXCLUDED.total_orders,
    total_amount = EXCLUDED.total_amount,
    paid_orders = EXCLUDED.paid_orders,
    paid_amount = EXCLUDED.paid_amount,
    cancelled_orders = EXCLUDED.cancelled_orders,
    cancelled_amount = EXCLUDED.cancelled_amount,
    updated_at = CURRENT_TIMESTAMP;
"""

def create_target_table(**context):
    hook = PostgresHook(postgres_conn_id='demo-postgres-warehouse')
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()
        logger.info("Target table created/verified successfully")
    finally:
        conn.close()

def aggregate_orders(**context):
    ds = context['ds']
    sql = AGGREGATE_SQL.replace("{{ ds }}", ds)
    hook = PostgresHook(postgres_conn_id='demo-postgres-warehouse')
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
            logger.info(f"Aggregation completed for {ds}")
    finally:
        conn.close()

create_table_task = PythonOperator(
    task_id='create_target_table',
    python_callable=create_target_table,
    dag=dag,
)

aggregate_task = PythonOperator(
    task_id='aggregate_orders',
    python_callable=aggregate_orders,
    dag=dag,
)

create_table_task >> aggregate_task