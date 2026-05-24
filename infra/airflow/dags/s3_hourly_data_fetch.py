from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_hourly_data_fetch',
    default_args=default_args,
    description='Ежечасная загрузка данных из S3',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['s3', 'hourly', 'etl'],
)

def fetch_s3_data(**context):
    """Забирает данные из S3 bucket"""
    hook = S3Hook(aws_conn_id='aws_default')
    
    # Пример: список файлов в bucket за последний час
    bucket_name = 'my-data-bucket'
    prefix = f"data/{context['execution_date'].strftime('%Y/%m/%d/%H')}/"
    
    keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    
    if keys:
        print(f"Найдено файлов: {len(keys)}")
        for key in keys:
            print(f"  - {key}")
            # Можно скачать файл:
            # content = hook.read_key(key=key, bucket_name=bucket_name)
    else:
        print(f"Файлов не найдено для префикса: {prefix}")
    
    return {'files_found': len(keys), 'prefix': prefix}

fetch_task = PythonOperator(
    task_id='fetch_s3_data',
    python_callable=fetch_s3_data,
    provide_context=True,
    dag=dag,
)

fetch_task
