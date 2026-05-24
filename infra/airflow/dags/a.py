from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'a',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Задача с bash-командой
task = BashOperator(
    task_id='run_command',
    bash_command='echo "Hello from DAG a"',
    dag=dag,
)