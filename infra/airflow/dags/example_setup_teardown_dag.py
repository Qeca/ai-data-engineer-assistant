from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='example_setup_teardown_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example', 'setup-teardown'],
)

# Setup tasks
setup_init = BashOperator(
    task_id='setup_init',
    bash_command='echo "=== SETUP: Initializing environment ===" && mkdir -p /tmp/dag_workdir',
    dag=dag,
)

setup_prepare = BashOperator(
    task_id='setup_prepare',
    bash_command='echo "=== SETUP: Preparing data ===" && echo "data_placeholder" > /tmp/dag_workdir/input.txt',
    dag=dag,
)

# Main tasks
task_process_1 = BashOperator(
    task_id='task_process_1',
    bash_command='echo "=== TASK 1: Processing data ===" && cat /tmp/dag_workdir/input.txt && echo "Processing complete"',
    dag=dag,
)

task_process_2 = BashOperator(
    task_id='task_process_2',
    bash_command='echo "=== TASK 2: Transforming data ===" && echo "transformed_data" > /tmp/dag_workdir/output.txt && cat /tmp/dag_workdir/output.txt',
    dag=dag,
)

# Teardown tasks
teardown_cleanup = BashOperator(
    task_id='teardown_cleanup',
    bash_command='echo "=== TEARDOWN: Cleaning up temporary files ===" && rm -f /tmp/dag_workdir/input.txt /tmp/dag_workdir/output.txt',
    dag=dag,
)

teardown_finalize = BashOperator(
    task_id='teardown_finalize',
    bash_command='echo "=== TEARDOWN: Finalizing - removing workdir ===" && rmdir /tmp/dag_workdir && echo "DAG execution complete"',
    dag=dag,
)

# Define task dependencies
setup_init >> setup_prepare >> task_process_1 >> task_process_2 >> teardown_cleanup >> teardown_finalize
