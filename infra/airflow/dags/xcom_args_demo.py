from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'xcom_args_demo',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['xcom', 'demo'],
)

# Task 1: Produce data - return value is automatically pushed to XCom with key 'return_value'
task1 = BashOperator(
    task_id='produce_data',
    bash_command='echo "data_from_task_1"',
    dag=dag,
)

# Task 2: Consume data from Task 1 via XCom
task2 = BashOperator(
    task_id='consume_task1_data',
    bash_command='echo "Received from task 1: {{ ti.xcom_pull(task_ids=\'produce_data\', key=\'return_value\') }}"',
    dag=dag,
)

# Task 3: Process and produce new data
task3 = BashOperator(
    task_id='process_data',
    bash_command='echo "processed_by_task_3"',
    dag=dag,
)

# Task 4: Final task consuming processed data from Task 3
task4 = BashOperator(
    task_id='final_task',
    bash_command='echo "Final task received: {{ ti.xcom_pull(task_ids=\'process_data\', key=\'return_value\') }}"',
    dag=dag,
)

# Define task dependencies
task1 >> task2 >> task3 >> task4
