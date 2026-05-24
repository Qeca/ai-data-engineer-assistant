from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='xcom_args_example',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['example', 'xcom'],
) as dag:

    # Task 1: Generate initial data and pass via XCom
    task1 = BashOperator(
        task_id='generate_data',
        bash_command='echo "initial_data_123"',
    )

    # Task 2: Receive data from task1 and transform it
    task2 = BashOperator(
        task_id='transform_data',
        bash_command='echo "transformed_{{ task_instance.xcom_pull(task_ids="generate_data") }}"',
    )

    # Task 3: Process the transformed data
    task3 = BashOperator(
        task_id='process_data',
        bash_command='echo "processed_{{ task_instance.xcom_pull(task_ids="transform_data") }}"',
    )

    # Task 4: Final output with all data
    task4 = BashOperator(
        task_id='final_output',
        bash_command='echo "final_result_{{ task_instance.xcom_pull(task_ids="process_data") }}"',
    )

    # Define task dependencies
    task1 >> task2 >> task3 >> task4
