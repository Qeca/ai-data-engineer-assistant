from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import BaseOperator
from datetime import datetime


class GetRequestOperator(BaseOperator):
    """Custom operator to perform HTTP GET request."""
    
    template_fields = ('url',)
    
    def __init__(self, url, **kwargs):
        super().__init__(**kwargs)
        self.url = url
    
    def execute(self, context):
        self.log.info(f"Making GET request to: {self.url}")
        import requests
        response = requests.get(self.url, timeout=30)
        response.raise_for_status()
        self.log.info(f"Response status: {response.status_code}")
        return response.text


default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'custom_operators_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['custom', 'demo'],
)

task1 = BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello from BashOperator" && date',
    dag=dag,
)

task2 = GetRequestOperator(
    task_id='get_request_task',
    url='https://httpbin.org/get',
    dag=dag,
)

task1 >> task2
