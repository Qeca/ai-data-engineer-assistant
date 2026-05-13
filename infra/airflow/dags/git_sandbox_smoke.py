from airflow import DAG
from airflow.operators.python import PythonOperator

def my_task():
    print('Hello from my task!')

with DAG('git_sandbox_smoke', schedule=None, catchup=False) as dag:
    task1 = PythonOperator(
        task_id='my_task',
        python_callable=my_task
    )