from airflow import DAG
from airflow.operators.bash import BashOperator
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
    'complex_dag_37_tasks',
    default_args=default_args,
    description='Complex DAG with exactly 37 tasks using BashOperator',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'complex', 'bash'],
)

# Task count: 37 total
# Structure:
# start: 1
# level1: 6
# level2: 10
# level3: 12
# level4: 6
# level5: 1
# end: 1
# Total: 1 + 6 + 10 + 12 + 6 + 1 + 1 = 37

# Start task (1)
start = BashOperator(
    task_id='start',
    bash_command='echo "Starting complex DAG with 37 tasks"',
    dag=dag,
)

# Level 1: 6 parallel tasks
level1_tasks = []
for i in range(1, 7):
    task = BashOperator(
        task_id=f'level1_task_{i}',
        bash_command=f'echo "Level 1, Task {i} - $(date)"',
        dag=dag,
    )
    level1_tasks.append(task)
    start >> task

# Level 2: 10 parallel tasks
level2_tasks = []
for i in range(10):
    task = BashOperator(
        task_id=f'level2_task_{i+1}',
        bash_command=f'echo "Level 2, Task {i+1} - Processing data batch {i+1}"',
        dag=dag,
    )
    level2_tasks.append(task)
    # Each level2 task depends on corresponding level1 task
    level1_tasks[i % 6] >> task

# Level 3: 12 parallel tasks
level3_tasks = []
for i in range(12):
    task = BashOperator(
        task_id=f'level3_task_{i+1}',
        bash_command=f'echo "Level 3, Task {i+1} - Transforming data {i+1}"',
        dag=dag,
    )
    level3_tasks.append(task)
    # Each level3 task depends on corresponding level2 task
    level2_tasks[i % 10] >> task

# Level 4: 6 parallel tasks
level4_tasks = []
for i in range(6):
    task = BashOperator(
        task_id=f'level4_task_{i+1}',
        bash_command=f'echo "Level 4, Task {i+1} - Aggregating results {i+1}"',
        dag=dag,
    )
    level4_tasks.append(task)
    # Each level4 task depends on 2 level3 tasks
    start_idx = i * 2
    for j in range(2):
        if start_idx + j < len(level3_tasks):
            level3_tasks[start_idx + j] >> task

# Level 5: 1 task (final aggregation)
level5 = BashOperator(
    task_id='level5_final_aggregation',
    bash_command='echo "Level 5 - Final aggregation of all results"',
    dag=dag,
)

# All level4 tasks converge to level5
for task in level4_tasks:
    task >> level5

# End task (1)
end = BashOperator(
    task_id='end',
    bash_command='echo "Complex DAG completed successfully! All 37 tasks finished."',
    dag=dag,
)

# Level5 converges to end
level5 >> end

# Verification:
# start: 1 task
# level1_tasks: 6 tasks (level1_task_1 to level1_task_6)
# level2_tasks: 10 tasks (level2_task_1 to level2_task_10)
# level3_tasks: 12 tasks (level3_task_1 to level3_task_12)
# level4_tasks: 6 tasks (level4_task_1 to level4_task_6)
# level5: 1 task (level5_final_aggregation)
# end: 1 task
# TOTAL: 1 + 6 + 10 + 12 + 6 + 1 + 1 = 37 tasks