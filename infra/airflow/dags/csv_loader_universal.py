from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import glob
from sqlalchemy import create_engine

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 24),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'csv_loader_universal',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Универсальная загрузка CSV файлов в PostgreSQL',
    tags=['csv', 'etl', 'postgres'],
)

def load_csv_to_postgres(**context):
    """
    Загружает все CSV файлы из /data/ в таблицу sales.raw_import
    """
    # Коннект к PostgreSQL
    engine = create_engine(
        'postgresql://demo:demo@demo-postgres:5432/analytics',
        echo=False
    )
    
    csv_dir = '/data'
    csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))
    
    if not csv_files:
        print(f'CSV файлы не найдены в {csv_dir}')
        return {'loaded_files': 0, 'total_rows': 0}
    
    total_rows = 0
    loaded_files = 0
    
    for csv_file in csv_files:
        try:
            print(f'Загрузка файла: {csv_file}')
            df = pd.read_csv(csv_file)
            
            # Загрузка в таблицу raw_import
            df.to_sql(
                'raw_import',
                engine,
                schema='sales',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            rows = len(df)
            total_rows += rows
            loaded_files += 1
            print(f'Загружено {rows} строк из {csv_file}')
            
        except Exception as e:
            print(f'Ошибка при загрузке {csv_file}: {str(e)}')
            raise
    
    return {'loaded_files': loaded_files, 'total_rows': total_rows}


load_task = PythonOperator(
    task_id='load_csv_to_db',
    python_callable=load_csv_to_postgres,
    dag=dag,
)

load_task