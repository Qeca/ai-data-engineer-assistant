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
    's3_hourly_data_pipeline',
    default_args=default_args,
    description='Ежечасная загрузка данных из S3',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['s3', 'hourly', 'etl'],
)


def fetch_s3_data(**context):
    """Загрузка данных из S3 bucket"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Параметры S3 - можно настроить через Variables или Parameters
    bucket_name = 'my-data-bucket'
    prefix = 'data/'
    
    # Получаем список файлов за последний час
    execution_date = context['execution_date']
    hour_prefix = execution_date.strftime('%Y/%m/%d/%H')
    full_prefix = f'{prefix}{hour_prefix}/'
    
    # Логирование
    print(f'Загрузка данных из S3://{bucket_name}/{full_prefix}')
    
    # Получаем список ключей
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=full_prefix)
    
    if not keys:
        print(f'Файлы не найдены по префиксу {full_prefix}')
        return {'files_found': 0, 'keys': []}
    
    print(f'Найдено файлов: {len(keys)}')
    
    # Загружаем каждый файл (пример обработки)
    for key in keys:
        # Можно скачать файл или прочитать напрямую
        # file_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
        print(f'Обработка файла: {key}')
    
    return {
        'files_found': len(keys),
        'keys': keys,
        'prefix': full_prefix
    }


fetch_task = PythonOperator(
    task_id='fetch_s3_data',
    python_callable=fetch_s3_data,
    provide_context=True,
    dag=dag,
)

fetch_task
