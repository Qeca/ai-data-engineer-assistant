from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bash_tutorial_dag',
    default_args=default_args,
    description='Tutorial DAG with BashOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'bash'],
)

task1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

task2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    dag=dag,
)

task3 = BashOperator(
    task_id='list_files',
    bash_command='ls -la /tmp',
    dag=dag,
)

task1 >> task2 >> task3