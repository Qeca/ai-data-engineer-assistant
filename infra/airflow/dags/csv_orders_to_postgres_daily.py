from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import glob
import csv
import psycopg2

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
    'csv_orders_to_postgres_daily',
    default_args=default_args,
    description='Ежедневная загрузка CSV из /data/orders/*.csv в таблицу sales.orders_raw Postgres',
    schedule_interval='@daily',
    catchup=False,
)

def load_csv_to_orders_raw():
    """Читает CSV файлы из /data/orders/*.csv и загружает в таблицу sales.orders_raw"""
    # Подключение к PostgreSQL
    conn = psycopg2.connect(
        host='demo-postgres',
        port=5432,
        database='analytics',
        user='demo',
        password='demo'
    )
    cur = conn.cursor()
    
    try:
        # Находим все CSV файлы
        csv_files = glob.glob('/data/orders/*.csv')
        
        if not csv_files:
            print("CSV файлы не найдены в /data/orders/")
            return
        
        rows_loaded = 0
        for csv_file in csv_files:
            print(f"Обработка файла: {csv_file}")
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Вставляем строку в таблицу
                    cur.execute("""
                        INSERT INTO sales.orders_raw (order_id, customer_id, order_date, amount, status, created_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (order_id) DO NOTHING
                    """, (
                        row.get('order_id'),
                        row.get('customer_id'),
                        row.get('order_date'),
                        row.get('amount'),
                        row.get('status')
                    ))
                    rows_loaded += 1
        
        conn.commit()
        print(f"Загружено файлов: {len(csv_files)}, строк: {rows_loaded}")
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

load_task = PythonOperator(
    task_id='load_csv_to_orders_raw',
    python_callable=load_csv_to_orders_raw,
    dag=dag,
)

load_task