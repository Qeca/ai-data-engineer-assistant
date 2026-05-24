from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Функция для этапа Extract
def extract_data(**context):
    """Извлечение данных из источника"""
    print("Starting Extract phase...")
    data = {
        "users": [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com"}
        ]
    }
    context['ti'].xcom_push(key='extracted_data', value=data)
    print(f"Extracted {len(data['users'])} users")
    return data

# Функция для этапа Transform
def transform_data(**context):
    """Трансформация данных"""
    print("Starting Transform phase...")
    ti = context['ti']
    raw_data = ti.xcom_pull(key='extracted_data', task_ids='extract_task')
    
    # Трансформируем данные: добавляем домен email и верхний регистр имени
    transformed_users = []
    for user in raw_data['users']:
        transformed_user = {
            "id": user["id"],
            "name": user["name"].upper(),
            "email": user["email"],
            "domain": user["email"].split("@")[1]
        }
        transformed_users.append(transformed_user)
    
    transformed_data = {"users": transformed_users}
    ti.xcom_push(key='transformed_data', value=transformed_data)
    print(f"Transformed {len(transformed_users)} users")
    return transformed_data

# Функция для этапа Load
def load_data(**context):
    """Загрузка данных в целевую систему"""
    print("Starting Load phase...")
    ti = context['ti']
    data = ti.xcom_pull(key='transformed_data', task_ids='transform_task')
    
    # Имитация загрузки в базу данных
    for user in data['users']:
        print(f"Loading user: {user['id']} - {user['name']} - {user['email']} - {user['domain']}")
    
    print(f"Successfully loaded {len(data['users'])} users to destination")
    return {"status": "success", "loaded_count": len(data['users'])}

# Определение DAG
default_args = {
    'owner': 'tutorial',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='etl_tutorial_dag',
    default_args=default_args,
    description='ETL Tutorial: Extract -> Transform -> Load pipeline demonstration',
    schedule_interval=None,  # No schedule, manual trigger only
    catchup=False,
    tags=['tutorial', 'etl', 'demo'],
)

# Создание задач
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Определение зависимостей задач
extract_task >> transform_task >> load_task
