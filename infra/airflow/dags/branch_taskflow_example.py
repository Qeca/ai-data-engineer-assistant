from airflow import DAG
from airflow.decorators import task, branch
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='branch_taskflow_example',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    catchup=False,
    tags=['example', 'branch', 'taskflow'],
) as dag:

    @branch
    @task
    def decide_path(**context):
        """
        Branching task that decides which path to take based on run number.
        Uses depends_on_past=True, so tasks may be run or skipped on alternating runs.
        """
        # Get the run number from context
        run_number = context['dag_run'].run_number
        
        # Alternate between paths on each run
        if run_number % 2 == 0:
            return 'task_even_path'
        else:
            return 'task_odd_path'

    task_even_path = EmptyOperator(
        task_id='task_even_path',
        dag=dag,
    )

    task_odd_path = EmptyOperator(
        task_id='task_odd_path',
        dag=dag,
    )

    task_join = EmptyOperator(
        task_id='task_join',
        trigger_rule='none_failed_min_one_success',
        dag=dag,
    )

    # Set dependencies
    decide_path() >> [task_even_path, task_odd_path] >> task_join
