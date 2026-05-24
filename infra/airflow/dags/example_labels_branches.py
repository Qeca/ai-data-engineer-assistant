from airflow import DAG
from airflow.operators.empty import EmptyOperator
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
    'example_labels_branches',
    default_args=default_args,
    description='Example DAG demonstrating the usage of labels with different branches',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'labels', 'branches'],
)

# Task 1: Start
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Task 2: Branch A - Process
branch_a_process = EmptyOperator(
    task_id='branch_a_process',
    dag=dag,
)

# Task 3: Branch A - Validate
branch_a_validate = EmptyOperator(
    task_id='branch_a_validate',
    dag=dag,
)

# Task 4: Branch B - Process
branch_b_process = EmptyOperator(
    task_id='branch_b_process',
    dag=dag,
)

# Task 5: Branch B - Validate
branch_b_validate = EmptyOperator(
    task_id='branch_b_validate',
    dag=dag,
)

# Task 6: Branch C - Process
branch_c_process = EmptyOperator(
    task_id='branch_c_process',
    dag=dag,
)

# Task 7: End
end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Define dependencies with labels
start >> branch_a_process >> branch_a_validate >> end
start >> branch_b_process >> branch_b_validate >> end
start >> branch_c_process >> end
