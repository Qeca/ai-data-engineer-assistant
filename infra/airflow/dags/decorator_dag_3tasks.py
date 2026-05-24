from airflow.decorators import dag, task
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'decorators']
)
def decorator_dag_3tasks():
    @task
    def extract_data():
        """Задача 1: Извлечение данных"""
        data = {"source": "api", "records": 100}
        return data
    
    @task
    def transform_data(input_data):
        """Задача 2: Трансформация данных"""
        transformed = {
            "source": input_data["source"].upper(),
            "records": input_data["records"] * 2,
            "processed_at": datetime.now().isoformat()
        }
        return transformed
    
    @task
    def load_data(input_data):
        """Задача 3: Загрузка данных"""
        result = f"Loaded {input_data['records']} records from {input_data['source']}"
        print(result)
        return result
    
    # Определение зависимостей между задачами
    data = extract_data()
    transformed = transform_data(data)
    load_data(transformed)

dag_instance = decorator_dag_3tasks()