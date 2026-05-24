from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'xcom_example_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    description='Example DAG demonstrating the usage of XComs with BashOperator',
)

# Task 1: Producer - writes data to XCom
task1_producer = BashOperator(
    task_id='producer',
    bash_command='echo "initial_data_123"',
    do_xcom_push=True,
    dag=dag,
)

# Task 2: Consumer - reads data from XCom
task2_consumer = BashOperator(
    task_id='consumer',
    bash_command='echo "Received: {{ task_instance.xcom_pull(task_ids="producer") }}"',
    dag=dag,
)

# Task 3: Transformer - processes data and writes back to XCom
task3_transformer = BashOperator(
    task_id='transformer',
    bash_command='echo "transformed_{{ task_instance.xcom_pull(task_ids="producer") }}"',
    do_xcom_push=True,
    dag=dag,
)

# Task 4: Consumer2 - reads transformed data
task4_consumer2 = BashOperator(
    task_id='consumer2',
    bash_command='echo "Transformed data: {{ task_instance.xcom_pull(task_ids="transformer") }}"',
    dag=dag,
)

# Task 5: Aggregator - combines multiple XCom values
task5_aggregator = BashOperator(
    task_id='aggregator',
    bash_command='echo "Aggregated: {{ task_instance.xcom_pull(task_ids="producer") }} + {{ task_instance.xcom_pull(task_ids="transformer") }}"',
    do_xcom_push=True,
    dag=dag,
)

# Task 6: Final - final task that uses all previous XComs
task6_final = BashOperator(
    task_id='final',
    bash_command='echo "Final result: {{ task_instance.xcom_pull(task_ids="aggregator") }}"',
    dag=dag,
)

# Define task dependencies
task1_producer >> task2_consumer >> task3_transformer >> task4_consumer2 >> task5_aggregator >> task6_final
