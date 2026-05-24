from airflow import DAG
from airflow.providers.redis.operators.redis_publish import RedisPublishOperator
from airflow.providers.redis.sensors.redis_pub_sub import RedisPubSubSensor
from airflow.providers.redis.sensors.redis_key import RedisKeySensor
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='redis_example_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['redis', 'example'],
)

# Task 1: Publish a message to Redis
publish_task = RedisPublishOperator(
    task_id='publish_message',
    channel='test_channel',
    message='Hello from Airflow!',
    redis_conn_id='redis_default',
    dag=dag,
)

# Task 2: Wait for a message on a Redis PubSub channel
sensor_pubsub = RedisPubSubSensor(
    task_id='wait_for_message',
    channels=['test_channel'],
    pattern='Hello*',
    redis_conn_id='redis_default',
    poke_interval=5,
    timeout=60,
    dag=dag,
)

# Task 3: Wait for a Redis key to exist
sensor_key = RedisKeySensor(
    task_id='wait_for_key',
    key='test_key',
    redis_conn_id='redis_default',
    poke_interval=5,
    timeout=60,
    dag=dag,
)

# Define task dependencies
publish_task >> sensor_pubsub >> sensor_key
