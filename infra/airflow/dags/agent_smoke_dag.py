from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task():
    print('Hello from my_task!')

def calculate_one_plus_one():
    result = 1 + 1
    print(f'1 + 1 = {result}')

with DAG(
    dag_id='agent_smoke_dag',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=my_task,
    )
    
    task2 = PythonOperator(
        task_id='calculate_sum',
        python_callable=calculate_one_plus_one,
    )
    
    task1 >> task2