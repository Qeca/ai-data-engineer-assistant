from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'xcom_demo_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    description='Example DAG demonstrating the usage of XComs with BashOperator',
)

# Task 1: Push initial value to XCom
task1 = BashOperator(
    task_id='push_initial_value',
    bash_command='echo "{{ ti.xcom_push(key="initial_value", value=100) }}"',
    dag=dag,
)

# Task 2: Pull value, add 50, push result
task2 = BashOperator(
    task_id='add_fifty',
    bash_command='''
    VALUE=$(echo "{{ ti.xcom_pull(key='initial_value', task_ids='push_initial_value') }}")
    RESULT=$((VALUE + 50))
    echo "{{ ti.xcom_push(key='after_add_50', value=$RESULT) }}"
    echo "Added 50: $RESULT"
    ''',
    dag=dag,
)

# Task 3: Pull value, multiply by 2, push result
task3 = BashOperator(
    task_id='multiply_by_two',
    bash_command='''
    VALUE=$(echo "{{ ti.xcom_pull(key='after_add_50', task_ids='add_fifty') }}")
    RESULT=$((VALUE * 2))
    echo "{{ ti.xcom_push(key='after_multiply', value=$RESULT) }}"
    echo "Multiplied by 2: $RESULT"
    ''',
    dag=dag,
)

# Task 4: Pull value, subtract 30, push result
task4 = BashOperator(
    task_id='subtract_thirty',
    bash_command='''
    VALUE=$(echo "{{ ti.xcom_pull(key='after_multiply', task_ids='multiply_by_two') }}")
    RESULT=$((VALUE - 30))
    echo "{{ ti.xcom_push(key='after_subtract', value=$RESULT) }}"
    echo "Subtracted 30: $RESULT"
    ''',
    dag=dag,
)

# Task 5: Pull value, divide by 3, push result
task5 = BashOperator(
    task_id='divide_by_three',
    bash_command='''
    VALUE=$(echo "{{ ti.xcom_pull(key='after_subtract', task_ids='subtract_thirty') }}")
    RESULT=$((VALUE / 3))
    echo "{{ ti.xcom_push(key='final_result', value=$RESULT) }}"
    echo "Divided by 3: $RESULT"
    ''',
    dag=dag,
)

# Task 6: Pull final value and display summary
task6 = BashOperator(
    task_id='display_summary',
    bash_command='''
    FINAL=$(echo "{{ ti.xcom_pull(key='final_result', task_ids='divide_by_three') }}")
    echo "========================================="
    echo "XCom Demo Summary"
    echo "========================================="
    echo "Initial value: 100"
    echo "After +50: 150"
    echo "After *2: 300"
    echo "After -30: 270"
    echo "After /3: 90"
    echo "========================================="
    echo "Final result from XCom: $FINAL"
    echo "XCom demonstration completed successfully!"
    ''',
    dag=dag,
)

# Define task dependencies
task1 >> task2 >> task3 >> task4 >> task5 >> task6
