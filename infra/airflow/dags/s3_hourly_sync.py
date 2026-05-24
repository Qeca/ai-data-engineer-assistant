from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    's3_hourly_sync',
    default_args=default_args,
    description='Ежечасная синхронизация данных из S3',
    schedule_interval='@hourly',
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=['s3', 'hourly', 'sync'],
) as dag:

    def fetch_s3_data(**context):
        """Загрузка данных из S3"""
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        # Пример: получение списка файлов из бакета
        bucket_name = 'my-data-bucket'
        prefix = 'incoming/'
        
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        
        if keys:
            print(f"Найдено файлов: {len(keys)}")
            for key in keys:
                print(f"  - {key}")
            
            # Пример чтения содержимого последнего файла
            if keys:
                latest_key = sorted(keys)[-1]
                content = s3_hook.read_key(key=latest_key, bucket_name=bucket_name)
                print(f"Содержимое файла {latest_key}:")
                print(content[:500] if len(content) > 500 else content)
        else:
            print("Файлы не найдены")
        
        return {'files_found': len(keys) if keys else 0}

    sync_task = PythonOperator(
        task_id='fetch_s3_data',
        python_callable=fetch_s3_data,
    )

    sync_task
