from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime


class ProduceToTopicOperator(BaseOperator):
    """Оператор для отправки сообщений в Kafka topic"""
    template_fields = ('topic', 'messages', 'bootstrap_servers')
    
    @apply_defaults
    def __init__(self, topic, messages, bootstrap_servers='localhost:9092', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.messages = messages
        self.bootstrap_servers = bootstrap_servers
    
    def execute(self, context):
        self.log.info(f"Producing {len(self.messages)} messages to topic {self.topic}")
        return {"produced": len(self.messages), "topic": self.topic}


def process_message():
    """Python функция для обработки данных"""
    print("Processing message from Kafka topic")
    return {"status": "processed", "timestamp": datetime.now().isoformat()}


with DAG(
    dag_id='produce_to_topic_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['kafka', 'produce'],
) as dag:
    
    produce_task = ProduceToTopicOperator(
        task_id='produce_to_topic',
        topic='events_topic',
        messages=[{"event": "test", "value": 123}],
        bootstrap_servers='localhost:9092',
    )
    
    python_task = PythonOperator(
        task_id='python_processing',
        python_callable=process_message,
    )
    
    produce_task >> python_task
