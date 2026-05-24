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

dag = DAG(
    's3_data_hourly_load',
    default_args=default_args,
    description='Ежечасная загрузка данных из S3',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['s3', 'hourly', 'etl'],
)


def fetch_s3_data(**context):
    """
    Загружает данные из S3 bucket.
    Настройте bucket_name и prefix под ваши нужды.
    """
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Параметры для настройки
    bucket_name = 'my-data-bucket'
    prefix = 'incoming/'
    
    # Получаем список объектов в S3
    keys = s3_hook.list_keys(
        bucket_name=bucket_name,
        prefix=prefix,
    )
    
    if not keys:
        print(f'Нет новых файлов в s3://{bucket_name}/{prefix}')
        return
    
    print(f'Найдено файлов: {len(keys)}')
    
    # Загружаем каждый файл
    for key in keys:
        # Читаем объект из S3
        obj = s3_hook.get_key(key=key, bucket_name=bucket_name)
        data = obj.get()['Body'].read().decode('utf-8')
        
        # Здесь можно добавить логику обработки данных
        # Например, загрузку в базу данных или обработку
        print(f'Загружен файл: {key}')
        print(f'Размер данных: {len(data)} байт')
        
        # Опционально: переместить файл в processed/ после обработки
        # s3_hook.copy_object(
        #     source_bucket_name=bucket_name,
        #     source_key=key,
        #     dest_bucket_name=bucket_name,
        #     dest_key=key.replace('incoming/', 'processed/')
        # )
        # s3_hook.delete_object(key=key, bucket_name=bucket_name)
    
    return {'files_processed': len(keys)}


fetch_s3_task = PythonOperator(
    task_id='fetch_s3_data',
    python_callable=fetch_s3_data,
    provide_context=True,
    dag=dag,
)

fetch_s3_task
