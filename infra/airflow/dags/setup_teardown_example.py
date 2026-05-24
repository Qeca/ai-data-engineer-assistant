from airflow.decorators import dag, setup, task, task_group, teardown
from datetime import datetime
from airflow.utils.task_group import TaskGroup

@dag(
    dag_id="setup_teardown_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "setup-teardown"],
)
def setup_teardown_example_dag():
    
    @setup
    @task
    def setup_task():
        """Setup задача: инициализация ресурсов"""
        print("Выполняется setup задача...")
        print("Инициализация ресурсов перед основными задачами")
        return {"status": "setup_complete", "timestamp": datetime.now().isoformat()}
    
    @task_group
    def processing_group():
        @task
        def task1():
            """Первая задача обработки"""
            print("Выполняется task1...")
            return {"task": "task1", "result": "processed"}
        
        @task
        def task2():
            """Вторая задача обработки"""
            print("Выполняется task2...")
            return {"task": "task2", "result": "processed"}
        
        task1() >> task2()
    
    @task
    def task3():
        """Третья задача обработки"""
        print("Выполняется task3...")
        return {"task": "task3", "result": "finalized"}
    
    @teardown
    @task
    def teardown_task():
        """Teardown задача: очистка ресурсов"""
        print("Выполняется teardown задача...")
        print("Очистка ресурсов после завершения всех задач")
        return {"status": "teardown_complete", "timestamp": datetime.now().isoformat()}
    
    # Определение зависимостей
    setup_result = setup_task()
    processing_group_result = processing_group()
    task3_result = task3()
    teardown_result = teardown_task()
    
    setup_result >> processing_group_result >> task3_result >> teardown_result

dag = setup_teardown_example_dag()
