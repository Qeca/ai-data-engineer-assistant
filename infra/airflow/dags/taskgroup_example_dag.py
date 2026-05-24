from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='taskgroup_example_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example', 'taskgroup'],
)

@task(dag=dag)
def start_task():
    """Начальная задача DAG"""
    print("Starting the DAG workflow")
    return "started"

@task_group(group_id='data_processing', dag=dag)
def data_processing_group():
    """Группа задач для обработки данных"""
    
    @task
    def process_data():
        """Обработка данных"""
        print("Processing data...")
        return {"status": "processed", "records": 100}
    
    @task
    def validate_data():
        """Валидация данных"""
        print("Validating data...")
        return {"status": "validated", "errors": 0}
    
    process_data() >> validate_data()
    return validate_data()

@task_group(group_id='transformation', dag=dag)
def transformation_group():
    """Группа задач для трансформации"""
    
    @task
    def transform_a():
        """Трансформация A"""
        print("Running transformation A")
        return "transform_a_complete"
    
    @task
    def transform_b():
        """Трансформация B"""
        print("Running transformation B")
        return "transform_b_complete"
    
    transform_a()
    transform_b()
    return [transform_a(), transform_b()]

@task(dag=dag)
def end_task(process_result, transform_result):
    """Финальная задача DAG"""
    print(f"Workflow completed. Process: {process_result}, Transform: {transform_result}")
    return "completed"

# Определение зависимостей
start = start_task()
process = data_processing_group()
transform = transformation_group()
end = end_task(process, transform)

start >> process >> transform >> end