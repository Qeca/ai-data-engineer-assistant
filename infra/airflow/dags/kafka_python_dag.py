from airflow import DAG
from airflow.providers.kafka.operators.produce import ProduceToTopicOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def process_data():
    """Простая функция для обработки данных"""
    print("Обработка данных выполнена успешно")
    return {"status": "success", "timestamp": datetime.now().isoformat()}

default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='kafka_python_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['kafka', 'python'],
)

produce_task = ProduceToTopicOperator(
    task_id='produce_to_kafka',
    topics='test-topic',
    producer_config={'bootstrap.servers': 'localhost:9092'},
    value='{"message": "Hello from Airflow"}',
    dag=dag,
)

python_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

produce_task >> python_task