from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def check_paid_orders_last_hour():
    """Проверяет наличие оплаченных заказов за последний час.
    Если строк нет — задача падает с исключением для триггера алерта.
    """
    hook = PostgresHook(postgres_conn_id='demo_postgres_warehouse')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    query = """
    SELECT COUNT(*) 
    FROM sales.orders 
    WHERE status = 'paid' 
    AND order_ts >= NOW() - INTERVAL '1 hour'
    """
    cur.execute(query)
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    if count == 0:
        raise Exception(f"ALERT: No paid orders in the last hour! Count: {count}")
    
    print(f"OK: Paid orders in last hour: {count}")

with DAG(
    'hourly_paid_orders_check',
    default_args={
        'owner': 'data-engineer',
        'depends_on_past': False,
        'start_date': datetime(2026, 5, 24),
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='@hourly',
    catchup=False,
    description='Проверка наличия оплаченных заказов за последний час. Падает если count=0.',
) as dag:
    check_task = PythonOperator(
        task_id='check_paid_orders_last_hour',
        python_callable=check_paid_orders_last_hour,
    )
