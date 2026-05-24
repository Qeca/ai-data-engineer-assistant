from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_order_anomalies',
    default_args=default_args,
    description='Ежедневный анализ аномалий заказов: отмененные, высокие и низкие суммы',
    schedule_interval='@daily',
    catchup=False,
    tags=['orders', 'anomalies', 'daily'],
)

detect_order_anomalies = PostgresOperator(
    task_id='detect_order_anomalies',
    postgres_conn_id='demo-postgres-warehouse',
    sql="""
    SELECT 
        'Отмененные заказы' AS anomaly_category,
        order_id,
        customer_id,
        order_ts,
        amount,
        status,
        'Заказ отменен' AS reason
    FROM sales.orders
    WHERE status = 'cancelled'

    UNION ALL

    SELECT 
        'Высокие суммы' AS anomaly_category,
        order_id,
        customer_id,
        order_ts,
        amount,
        status,
        'Сумма > 2x средней (' || ROUND((SELECT AVG(amount) FROM sales.orders), 2) || ')' AS reason
    FROM sales.orders
    WHERE amount > (SELECT AVG(amount) * 2 FROM sales.orders)

    UNION ALL

    SELECT 
        'Низкие суммы' AS anomaly_category,
        order_id,
        customer_id,
        order_ts,
        amount,
        status,
        'Сумма < 0.5x средней' AS reason
    FROM sales.orders
    WHERE amount < (SELECT AVG(amount) * 0.5 FROM sales.orders)

    ORDER BY anomaly_category, amount DESC
    """,
    dag=dag,
)