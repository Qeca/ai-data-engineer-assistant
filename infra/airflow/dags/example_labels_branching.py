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

with DAG(
    dag_id='example_labels_branching',
    default_args=default_args,
    description='Example DAG demonstrating the usage of labels with different branches',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'labels', 'branching'],
) as dag:

    # Start task
    start = EmptyOperator(
        task_id='start',
        labels={'branch': 'main', 'priority': 'high'}
    )

    # Branch A tasks
    branch_a_1 = EmptyOperator(
        task_id='branch_a_step_1',
        labels={'branch': 'A', 'priority': 'medium'}
    )

    branch_a_2 = EmptyOperator(
        task_id='branch_a_step_2',
        labels={'branch': 'A', 'priority': 'medium'}
    )

    # Branch B tasks
    branch_b_1 = EmptyOperator(
        task_id='branch_b_step_1',
        labels={'branch': 'B', 'priority': 'low'}
    )

    branch_b_2 = EmptyOperator(
        task_id='branch_b_step_2',
        labels={'branch': 'B', 'priority': 'low'}
    )

    # Merge task
    merge = EmptyOperator(
        task_id='merge',
        labels={'branch': 'main', 'priority': 'high'}
    )

    # End task
    end = EmptyOperator(
        task_id='end',
        labels={'branch': 'main', 'priority': 'high'}
    )

    # Define dependencies
    start >> [branch_a_1, branch_b_1]
    branch_a_1 >> branch_a_2 >> merge
    branch_b_1 >> branch_b_2 >> merge
    merge >> end
