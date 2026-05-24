from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='example_trigger_controller_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example', 'trigger'],
)

# Задача 1: BashOperator - подготовка
prepare_task = BashOperator(
    task_id='prepare_trigger',
    bash_command='echo "Preparing to trigger target DAG..."',
    dag=dag,
)

# Задача 2: TriggerDagRunOperator - запуск целевого DAG
trigger_task = TriggerDagRunOperator(
    task_id='trigger_target_dag',
    trigger_dag_id='example_trigger_target_dag',
    dag=dag,
)

prepare_task >> trigger_task
