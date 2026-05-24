from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'orders_daily_aggregation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Агрегация заказов за день из sales.orders в analytics.daily_orders',
)

def create_target_table():
    """Создаёт целевую таблицу analytics.daily_orders если не существует"""
    conn = psycopg2.connect(
        host='demo-postgres',
        port=5432,
        database='analytics',
        user='demo',
        password='demo'
    )
    cur = conn.cursor()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS analytics.daily_orders (
        order_date DATE PRIMARY KEY,
        total_orders INTEGER,
        total_amount NUMERIC(15,2),
        avg_amount NUMERIC(15,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cur.execute(create_table_sql)
    conn.commit()
    cur.close()
    conn.close()
    print("Таблица analytics.daily_orders создана или уже существует")

def aggregate_orders():
    """Агрегирует заказы за предыдущий день из sales.orders в analytics.daily_orders"""
    conn = psycopg2.connect(
        host='demo-postgres',
        port=5432,
        database='analytics',
        user='demo',
        password='demo'
    )
    cur = conn.cursor()
    
    # Агрегация за предыдущий день
    aggregate_sql = """
    INSERT INTO analytics.daily_orders (order_date, total_orders, total_amount, avg_amount)
    SELECT 
        DATE(order_ts) as order_date,
        COUNT(*) as total_orders,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM sales.orders
    WHERE DATE(order_ts) = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY DATE(order_ts)
    ON CONFLICT (order_date) DO UPDATE SET
        total_orders = EXCLUDED.total_orders,
        total_amount = EXCLUDED.total_amount,
        avg_amount = EXCLUDED.avg_amount,
        created_at = CURRENT_TIMESTAMP
    """
    cur.execute(aggregate_sql)
    conn.commit()
    
    # Проверка результата
    cur.execute("SELECT * FROM analytics.daily_orders ORDER BY order_date DESC LIMIT 5")
    rows = cur.fetchall()
    print("Последние 5 записей в analytics.daily_orders:")
    for row in rows:
        print(row)
    
    cur.close()
    conn.close()
    print("Агрегация заказов завершена успешно")

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
