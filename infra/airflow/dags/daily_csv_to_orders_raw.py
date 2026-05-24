from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os
import csv
import glob

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_csv_to_orders_raw',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Ежедневная загрузка CSV из /data в orders_raw',
)

def get_csv_files(**context):
    """Находит CSV файлы в /data для обработки"""
    csv_files = glob.glob('/data/*.csv')
    if not csv_files:
        print('CSV файлы не найдены в /data, пропускаем')
        return []
    print(f'Найдены CSV файлы: {csv_files}')
    return csv_files

def prepare_insert_statements(**context):
    """Генерирует SQL INSERT statements из CSV файлов"""
    csv_files = context['ti'].xcom_pull(task_ids='find_csv_files')
    if not csv_files:
        return ''
    
    insert_statements = []
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                order_id = row.get('order_id', 'NULL')
                customer_id = row.get('customer_id', 'NULL')
                order_ts = row.get('order_ts', 'NULL')
                amount = row.get('amount', 'NULL')
                status = row.get('status', '')
                
                # Форматируем значения для SQL
                order_id_val = order_id if order_id != 'NULL' else 'NULL'
                customer_id_val = customer_id if customer_id != 'NULL' else 'NULL'
                order_ts_val = f"'{order_ts}'" if order_ts != 'NULL' else 'NULL'
                amount_val = amount if amount != 'NULL' else 'NULL'
                status_val = f"'{status}'"
                source_file_val = f"'{filename}'"
                
                insert_sql = f"""
                INSERT INTO sales.orders_raw 
                (order_id, customer_id, order_ts, amount, status, source_file, loaded_at)
                VALUES ({order_id_val}, {customer_id_val}, {order_ts_val}, {amount_val}, {status_val}, {source_file_val}, CURRENT_TIMESTAMP);
                """
                insert_statements.append(insert_sql)
    
    return '\n'.join(insert_statements)

find_csv_task = PythonOperator(
    task_id='find_csv_files',
    python_callable=get_csv_files,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_orders_raw_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS sales.orders_raw (
        order_id INTEGER,
        customer_id INTEGER,
        order_ts TIMESTAMP,
        amount NUMERIC,
        status TEXT,
        source_file TEXT,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

prepare_sql_task = PythonOperator(
    task_id='prepare_insert_statements',
    python_callable=prepare_insert_statements,
    dag=dag,
)

load_csv_task = PostgresOperator(
    task_id='load_csv_to_raw',
    postgres_conn_id='postgres_default',
    sql="{{ ti.xcom_pull(task_ids='prepare_insert_statements') }}",
    dag=dag,
)

find_csv_task >> create_table_task >> prepare_sql_task >> load_csv_task
