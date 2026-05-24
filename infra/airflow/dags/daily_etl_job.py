from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data(**context):
    """Извлечение данных из источников"""
    print("Starting data extraction...")
    return {'extracted': True, 'timestamp': datetime.now().isoformat()}

def transform_data(**context):
    """Трансформация данных"""
    print("Starting data transformation...")
    return {'transformed': True, 'timestamp': datetime.now().isoformat()}

def load_data(**context):
    """Загрузка данных в целевую систему"""
    print("Starting data loading...")
    return {'loaded': True, 'timestamp': datetime.now().isoformat()}

def validate_data(**context):
    """Валидация загруженных данных"""
    print("Validating loaded data...")
    return {'validated': True, 'timestamp': datetime.now().isoformat()}

dag = DAG(
    'daily_etl_job',
    default_args=default_args,
    description='Ежедневный ETL пайплайн: извлечение, трансформация, загрузка и валидация данных',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'daily', 'production'],
)

start = EmptyOperator(task_id='start', dag=dag)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

validate = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

start >> extract >> transform >> load >> validate >> end
