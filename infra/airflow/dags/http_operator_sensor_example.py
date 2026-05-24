from airflow import DAG
from airflow.operators.http_operator import HttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'http_operator_sensor_example',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['http', 'example'],
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

http_task_1 = HttpOperator(
    task_id='http_task_1',
    http_conn_id='http_default',
    endpoint='/api/v1/status',
    method='GET',
    dag=dag,
)

http_sensor_1 = HttpSensor(
    task_id='http_sensor_1',
    http_conn_id='http_default',
    endpoint='/api/v1/ready',
    method='GET',
    response_check=lambda response: response.status_code == 200,
    poke_interval=10,
    timeout=300,
    dag=dag,
)

http_task_2 = HttpOperator(
    task_id='http_task_2',
    http_conn_id='http_default',
    endpoint='/api/v1/process',
    method='POST',
    data={'action': 'process'},
    dag=dag,
)

http_sensor_2 = HttpSensor(
    task_id='http_sensor_2',
    http_conn_id='http_default',
    endpoint='/api/v1/complete',
    method='GET',
    response_check=lambda response: response.status_code == 200,
    poke_interval=10,
    timeout=300,
    dag=dag,
)

http_task_3 = HttpOperator(
    task_id='http_task_3',
    http_conn_id='http_default',
    endpoint='/api/v1/result',
    method='GET',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

start_task >> http_task_1 >> http_sensor_1 >> http_task_2 >> http_sensor_2 >> http_task_3 >> end_task
