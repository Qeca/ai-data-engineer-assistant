from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='decorator_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['example', 'decorators'],
)
def my_decorator_dag():
    @task
    def extract():
        """Извлечение данных"""
        return {'data': 'sample', 'timestamp': datetime.now().isoformat()}
    
    @task
    def transform(data):
        """Трансформация данных"""
        transformed = {
            'transformed': data,
            'processed_at': datetime.now().isoformat()
        }
        return transformed
    
    @task
    def load(data):
        """Загрузка данных"""
        print(f"Loading data: {data}")
        return 'load_complete'
    
    # Определение потока задач
    extract_data = extract()
    transform_data = transform(extract_data)
    load(transform_data)

dag = my_decorator_dag()