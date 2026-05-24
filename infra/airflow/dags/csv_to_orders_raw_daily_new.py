from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'csv_to_orders_raw_daily_new',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='Ежедневная загрузка CSV из /data в orders_raw',
)

def load_csv_to_orders_raw(**context):
    """Загрузка CSV файлов из /data в таблицу orders_raw"""
    import pandas as pd
    from sqlalchemy import create_engine, text
    import glob
    import os
    
    # Подключение к PostgreSQL
    engine = create_engine('postgresql://demo:demo@demo-postgres:5432/analytics')
    
    # Создание таблицы orders_raw если не существует
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
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    print('Таблица sales.orders_raw создана или уже существует')
    
    # Поиск всех CSV файлов в /data
    csv_files = glob.glob('/data/*.csv')
    
    if not csv_files:
        print('CSV файлы не найдены в /data')
        return
    
    print(f'Найдено CSV файлов: {len(csv_files)}')
    
    all_dfs = []
    for csv_file in csv_files:
        print(f'Чтение файла: {csv_file}')
        df = pd.read_csv(csv_file)
        all_dfs.append(df)
    
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f'Всего строк для загрузки: {len(combined_df)}')
        
        # Загрузка в таблицу orders_raw
        combined_df.to_sql(
            'orders_raw',
            engine,
            schema='sales',
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        print('Данные успешно загружены в sales.orders_raw')
    else:
        print('Нет данных для загрузки')

load_task = PythonOperator(
    task_id='load_csv_to_orders_raw',
    python_callable=load_csv_to_orders_raw,
    dag=dag,
)

load_task
