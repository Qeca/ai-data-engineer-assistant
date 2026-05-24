from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='example_trigger_target_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example', 'trigger', 'target'],
)

# Задача 1: BashOperator - начало выполнения
start_task = BashOperator(
    task_id='start_processing',
    bash_command='echo "Target DAG started by TriggerDagRunOperator!"',
    dag=dag,
)

# Задача 2: BashOperator - завершение выполнения
end_task = BashOperator(
    task_id='complete_processing',
    bash_command='echo "Target DAG completed successfully!"',
    dag=dag,
)

start_task >> end_task
