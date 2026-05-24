from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import glob
import os
from sqlalchemy import create_engine

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-engineering@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    'csv_to_orders_raw',
    default_args=default_args,
    description='Ежедневная загрузка CSV файлов в таблицу sales.orders_raw',
    schedule_interval='@daily',
    catchup=False,
)

def load_csv_to_orders_raw(**context):
    """Загрузка CSV файлов из /data/orders/*.csv в таблицу sales.orders_raw"""
    
    # Подключение к PostgreSQL
    engine = create_engine('postgresql://demo:demo@demo-postgres:5432/analytics')
    
    # Путь к CSV файлам
    csv_path = '/data/orders/*.csv'
    csv_files = glob.glob(csv_path)
    
    if not csv_files:
        print(f'CSV файлы не найдены по пути {csv_path}')
        return
    
    all_dfs = []
    for csv_file in csv_files:
        print(f'Чтение файла: {csv_file}')
        df = pd.read_csv(csv_file)
        all_dfs.append(df)
    
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f'Всего строк для загрузки: {len(combined_df)}')
        
        # Загрузка в таблицу orders_raw (создаст если не существует)
        combined_df.to_sql(
            'orders_raw',
            engine,
            schema='sales',
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        print(f'Успешно загружено {len(combined_df)} строк в sales.orders_raw')
    else:
        print('Нет данных для загрузки')

load_task = PythonOperator(
    task_id='load_csv_to_orders_raw',
    python_callable=load_csv_to_orders_raw,
    dag=dag,
)

load_task
