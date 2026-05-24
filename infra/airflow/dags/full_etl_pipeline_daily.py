from airflow import DAG
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
    'full_etl_pipeline_daily',
    default_args=default_args,
    description='Ежедневный ETL пайплайн: извлечение, трансформация и загрузка данных',
    schedule_interval='@daily',
    start_date=datetime(2026, 5, 21),
    catchup=False,
    tags=['etl', 'spark', 'daily'],
)

def run_spark_etl_job(**context):
    """Запускает Spark ETL джобу через API платформы"""
    import os
    import requests
    
    # Получаем токен из переменных окружения Airflow
    api_base = os.environ.get('PLATFORM_API_BASE', 'http://backend:8000')
    api_token = os.environ.get('PLATFORM_API_TOKEN', '')

    if not api_token:
        login_response = requests.post(
            f'{api_base}/auth/login',
            json={
                'email': os.environ.get('PLATFORM_API_EMAIL', 'admin@local.dev'),
                'password': os.environ.get('PLATFORM_API_PASSWORD', 'admin'),
            },
            timeout=15,
        )
        login_response.raise_for_status()
        api_token = login_response.json()['access_token']

    headers = {'Authorization': f'Bearer {api_token}'} if api_token else {}
    
    # Параметры Spark-джобы
    job_config = {
        'name': 'full_etl_pipeline_daily_run',
        'app_resource': 'full_etl_pipeline.py',
        'params': {
            'executor_memory': '2g',
            'partitions': 10,
            'source': 'airflow',
        },
    }
    
    # Отправляем запрос на запуск Spark-джобы
    response = requests.post(
        f'{api_base}/spark/jobs',
        json=job_config,
        headers=headers,
        timeout=15,
    )
    
    if response.status_code != 200:
        raise Exception(f'Failed to submit Spark job: {response.text}')
    
    job_result = response.json()
    job_id = job_result.get('job_id')
    
    # Ждем завершения джобы (polling)
    import time
    max_wait = 300  # 5 минут
    waited = 0
    
    while waited < max_wait:
        status_response = requests.get(
            f'{api_base}/spark/jobs/{job_id}',
            headers=headers,
            timeout=15,
        )
        status_response.raise_for_status()
        status = status_response.json().get('status', 'unknown')
        
        if status in ['success', 'failed', 'error']:
            if status != 'success':
                raise Exception(f'Spark job failed with status: {status}')
            break
        
        time.sleep(10)
        waited += 10
    
    if waited >= max_wait:
        raise Exception('Spark job timeout')
    
    return {'job_id': job_id, 'status': 'success'}

run_etl_task = PythonOperator(
    task_id='run_full_etl_spark_job',
    python_callable=run_spark_etl_job,
    provide_context=True,
    dag=dag,
)

run_etl_task
