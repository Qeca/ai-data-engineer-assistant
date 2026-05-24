from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

with DAG(
    dag_id='csv_loader_daily',
    default_args=default_args,
    description='Ежедневная загрузка CSV файлов в базу данных',
    schedule_interval='@daily',
    catchup=False,
    tags=['csv', 'etl', 'loading'],
) as dag:

    def load_csv_to_db(**context):
        """Загрузка CSV файла в таблицу PostgreSQL"""
        # Параметры из DAG run config или default
        csv_path = context['dag_run'].conf.get('csv_path', '/data/input/orders.csv')
        table_name = context['dag_run'].conf.get('table_name', 'orders_raw')
        schema = context['dag_run'].conf.get('schema', 'sales')
        
        print(f"Загрузка CSV: {csv_path}")
        print(f"Таблица: {schema}.{table_name}")
        
        # Чтение CSV
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV файл не найден: {csv_path}")
        
        df = pd.read_csv(csv_path)
        print(f"Прочитано строк: {len(df)}")
        print(f"Колонки: {list(df.columns)}")
        
        # Подключение к БД через Airflow connection
        from airflow.hooks.base import BaseHook
        from sqlalchemy import create_engine
        
        # Используем demo-postgres-warehouse connection
        conn = BaseHook.get_connection('demo_postgres_warehouse')
        
        # Формируем connection string
        conn_str = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        engine = create_engine(conn_str)
        
        # Загрузка в таблицу
        df.to_sql(
            name=table_name,
            schema=schema,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"Успешно загружено {len(df)} строк в {schema}.{table_name}")
        return {'rows_loaded': len(df), 'table': f'{schema}.{table_name}'}

    load_task = PythonOperator(
        task_id='load_csv_to_db',
        python_callable=load_csv_to_db,
        provide_context=True,
    )
