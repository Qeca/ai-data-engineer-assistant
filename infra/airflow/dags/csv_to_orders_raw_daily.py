from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import csv
import psycopg2

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_csv_to_orders_raw(**context):
    """Читает CSV файлы из /data и загружает в таблицу orders_raw"""
    
    # Параметры подключения к PostgreSQL
    conn_params = {
        'host': 'demo-postgres',
        'port': 5432,
        'database': 'analytics',
        'user': 'demo',
        'password': 'demo'
    }
    
    data_dir = '/data'
    loaded_files = []
    
    # Подключаемся к БД
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    
    try:
        # Создаем таблицу orders_raw если не существует
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS sales.orders_raw (
            order_id INTEGER,
            customer_id INTEGER,
            order_ts TIMESTAMP,
            amount NUMERIC,
            status TEXT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cur.execute(create_table_sql)
        conn.commit()
        
        # Ищем все CSV файлы в директории /data
        if os.path.exists(data_dir):
            csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
            
            for csv_file in csv_files:
                file_path = os.path.join(data_dir, csv_file)
                rows_loaded = 0
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    
                    for row in reader:
                        insert_sql = """
                        INSERT INTO sales.orders_raw (order_id, customer_id, order_ts, amount, status)
                        VALUES (%s, %s, %s, %s, %s)
                        """
                        cur.execute(insert_sql, (
                            row.get('order_id'),
                            row.get('customer_id'),
                            row.get('order_ts'),
                            row.get('amount'),
                            row.get('status')
                        ))
                        rows_loaded += 1
                
                conn.commit()
                loaded_files.append({'file': csv_file, 'rows': rows_loaded})
                print(f"Загружен файл {csv_file}: {rows_loaded} строк")
        else:
            print(f"Директория {data_dir} не существует")
            
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()
    
    return {'loaded_files': loaded_files}


with DAG(
    'csv_to_orders_raw_daily',
    default_args=default_args,
    description='Ежедневная загрузка CSV из /data в orders_raw',
    schedule_interval='@daily',
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=['etl', 'csv', 'orders'],
) as dag:
    
    load_task = PythonOperator(
        task_id='load_csv_to_orders_raw',
        python_callable=load_csv_to_orders_raw,
        provide_context=True,
    )
    
    load_task
