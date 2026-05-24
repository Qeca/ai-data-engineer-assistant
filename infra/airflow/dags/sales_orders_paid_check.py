from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sales_orders_paid_check',
    default_args=default_args,
    description='Проверка наличия оплаченных заказов за последний час',
    schedule_interval='@hourly',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['sales', 'monitoring', 'alerts'],
)

def check_paid_orders():
    """Проверяет, что за последний час есть хотя бы один заказ со status='paid'"""
    conn = psycopg2.connect(
        host='demo-postgres',
        port=5432,
        database='analytics',
        user='demo',
        password='demo'
    )
    
    try:
        with conn.cursor() as cur:
            query = """
            SELECT COUNT(*) as cnt
            FROM sales.orders
            WHERE status = 'paid'
              AND order_ts >= NOW() - INTERVAL '1 hour'
            """
            cur.execute(query)
            result = cur.fetchone()
            count = result[0] if result else 0
        
        if count == 0:
            raise Exception(f"ALERT: Нет оплаченных заказов за последний час (count={count})")
        
        print(f"OK: Найдено {count} оплаченных заказов за последний час")
        return count
    finally:
        conn.close()

check_task = PythonOperator(
    task_id='check_paid_orders_last_hour',
    python_callable=check_paid_orders,
    dag=dag,
)

check_task
