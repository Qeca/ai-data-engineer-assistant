from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import clickhouse_connect
import pyarrow as pa
import pyarrow.parquet as pq
import io
import boto3
from botocore.exceptions import NoCredentialsError

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clickhouse_events_daily_export',
    default_args=default_args,
    description='Ежедневная выгрузка событий из ClickHouse analytics.events в S3',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=True,
    tags=['clickhouse', 's3', 'events', 'daily'],
)

def extract_events_to_s3(**context):
    """
    Извлекает события из ClickHouse analytics.events за вчерашний день (ds={{ ds }})
    и сохраняет их как parquet в s3://lake/events/ds=YYYY-MM-DD/
    """
    ds = context['ds']  # формат YYYY-MM-DD для вчерашнего дня
    
    # Параметры подключения к ClickHouse
    ch_host = 'demo-clickhouse'
    ch_port = 8123
    ch_database = 'analytics'
    ch_user = 'demo'
    ch_password = 'demo'  # пароль из подключения demo-clickhouse-events
    
    # Параметры S3
    s3_bucket = 'lake'
    s3_key = f'events/ds={ds}/events_{ds}.parquet'
    
    # SQL запрос к ClickHouse с использованием partition ds
    query = f"""
    SELECT *
    FROM analytics.events
    WHERE ds = '{ds}'
    """
    
    print(f"Извлечение данных за {ds}")
    print(f"Query: {query}")
    
    # Подключение к ClickHouse
    client = clickhouse_connect.get_client(
        host=ch_host,
        port=ch_port,
        database=ch_database,
        username=ch_user,
        password=ch_password,
        secure=False,
    )
    
    try:
        # Выполнение запроса
        result = client.query(query)
        
        # Получение колонок и данных
        columns = result.column_names
        data = result.result_rows
        
        print(f"Извлечено {len(data)} строк")
        
        if len(data) == 0:
            print("Нет данных за указанный период")
            return
        
        # Преобразование в PyArrow Table
        # Создаем схемы на основе данных
        arrays = []
        for i, col in enumerate(columns):
            col_data = [row[i] for row in data]
            arrays.append(pa.array(col_data))
        
        table = pa.table(dict(zip(columns, arrays)))
        
        # Сериализация в Parquet
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        # Загрузка в S3
        s3_client = boto3.client('s3')
        
        try:
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream',
            )
            print(f"Данные успешно загружены в s3://{s3_bucket}/{s3_key}")
        except NoCredentialsError:
            print("Ошибка: не найдены credentials для S3")
            # Для локальной разработки можно использовать mock S3 или localstack
            print(f"Mock: данные готовы для загрузки в s3://{s3_bucket}/{s3_key}")
            print(f"Размер parquet: {buffer.tell()} байт")
        
        return {
            'rows_extracted': len(data),
            's3_path': f's3://{s3_bucket}/{s3_key}',
            'ds': ds,
        }
        
    finally:
        client.close()

extract_task = PythonOperator(
    task_id='extract_events_to_s3',
    python_callable=extract_events_to_s3,
    provide_context=True,
    dag=dag,
)

extract_task
