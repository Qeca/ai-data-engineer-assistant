from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.branch import BranchPythonOperator
from datetime import datetime, timedelta

def choose_branch_a_or_b(**context):
    """Выбирает ветку A или B на первом уровне ветвления."""
    return 'path_a_task'

def choose_branch_b1_or_b2(**context):
    """Выбирает подветку B1 или B2 на втором уровне ветвления."""
    return 'path_b_subtask_1'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    'nested_branching_example',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'branching'],
)

# Задача 1: start
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Задача 2: branch_1 - первый уровень ветвления
branch_1 = BranchPythonOperator(
    task_id='branch_1',
    python_callable=choose_branch_a_or_b,
    dag=dag,
)

# Задача 3: path_a_task - ветка A
path_a_task = EmptyOperator(
    task_id='path_a_task',
    dag=dag,
)

# Задача 4: path_b_branch - ветка B с вложенным ветвлением
path_b_branch = BranchPythonOperator(
    task_id='path_b_branch',
    python_callable=choose_branch_b1_or_b2,
    dag=dag,
)

# Задача 5: path_b_subtask_1 - подветка B1
path_b_subtask_1 = EmptyOperator(
    task_id='path_b_subtask_1',
    dag=dag,
)

# Задача 6: path_b_subtask_2 - подветка B2
path_b_subtask_2 = EmptyOperator(
    task_id='path_b_subtask_2',
    dag=dag,
)

# Задача 7: join - join task с trigger_rule='none_failed_min_one_success'
join = EmptyOperator(
    task_id='join',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Задача 8: end
end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Определение зависимостей
start >> branch_1
branch_1 >> path_a_task
branch_1 >> path_b_branch
path_a_task >> join
path_b_branch >> path_b_subtask_1
path_b_branch >> path_b_subtask_2
path_b_subtask_1 >> join
path_b_subtask_2 >> join
join >> end
