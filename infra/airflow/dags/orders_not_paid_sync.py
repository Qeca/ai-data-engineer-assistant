from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

import psycopg2

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'orders_not_paid_sync',
    default_args=default_args,
    description='Ежечасная синхронизация заказов со статусом не paid в таблицу orders_not_paid',
    schedule_interval='@hourly',
    start_date=datetime(2026, 5, 13),
    catchup=False,
    tags=['orders', 'sync'],
)

def sync_not_paid_orders(**context):
    host = os.getenv('AI_DE_DB_HOST', 'postgres')
    port = int(os.getenv('AI_DE_DB_PORT', '5432'))
    dbname = os.getenv('AI_DE_DB_NAME', 'ai_de')
    user = os.getenv('AI_DE_DB_USER', 'postgres')
    password = os.getenv('AI_DE_DB_PASSWORD', 'postgres')
    
    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    cursor = connection.cursor()
    
    try:
        # Создаем таблицу если не существует
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders_not_paid (
            id INTEGER PRIMARY KEY,
            created_at TIMESTAMP,
            user_id INTEGER,
            total_amount NUMERIC,
            status VARCHAR,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Очищаем таблицу перед полной перезагрузкой
        cursor.execute("TRUNCATE TABLE orders_not_paid;")
        
        # Вставляем все заказы где status != 'paid'
        cursor.execute("""
        INSERT INTO orders_not_paid (id, created_at, user_id, total_amount, status)
        SELECT id, created_at, user_id, total_amount, status
        FROM orders
        WHERE status != 'paid';
        """)
        
        connection.commit()
        rowcount = cursor.rowcount
        print(f"Синхронизировано заказов: {rowcount}")
        
    finally:
        cursor.close()
        connection.close()

sync_task = PythonOperator(
    task_id='sync_not_paid_orders',
    python_callable=sync_not_paid_orders,
    dag=dag,
)

sync_task
