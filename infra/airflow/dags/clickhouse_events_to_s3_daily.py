from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clickhouse_events_to_s3_daily',
    default_args=default_args,
    description='Ежедневная выгрузка событий из ClickHouse analytics.events в S3',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=True,
    tags=['clickhouse', 's3', 'events', 'etl'],
)


def extract_events_to_s3(**context):
    """
    Извлекает события из ClickHouse за вчерашний день и сохраняет в S3 как parquet.
    Использует макрос ds для partition ds={{ ds }}.
    """
    from clickhouse_driver import Client
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import boto3
    from io import BytesIO
    
    ds = context['ds']  # YYYY-MM-DD формат
    logging.info(f"Processing events for ds={ds}")
    
    # Подключение к ClickHouse
    client = Client(
        host='demo-clickhouse',
        port=9000,  # Native port для clickhouse-driver
        user='demo',
        password='demo',
        database='analytics',
    )
    
    # Запрос данных за указанный день из партиции
    query = f"""
    SELECT *
    FROM analytics.events
    WHERE ds = '{ds}'
    """
    
    logging.info(f"Executing query: {query}")
    
    # Выполнение запроса
    result = client.execute(query, with_column_types=True)
    rows = result[0]
    columns = result[1]
    
    if not rows:
        logging.info(f"No events found for ds={ds}")
        return {'status': 'success', 'rows': 0, 'message': 'No data'}
    
    # Создание DataFrame
    column_names = [col[0] for col in columns]
    df = pd.DataFrame(rows, columns=column_names)
    
    logging.info(f"Extracted {len(df)} rows")
    
    # Конвертация в PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # Запись в буфер как parquet
    buffer = BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)
    
    # Загрузка в S3
    s3_client = boto3.client('s3')
    s3_path = f"events/ds={ds}/events_{ds}.parquet"
    bucket = 'lake'
    
    logging.info(f"Uploading to s3://{bucket}/{s3_path}")
    
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_path,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream',
    )
    
    logging.info(f"Successfully uploaded {len(df)} rows to s3://{bucket}/{s3_path}")
    
    return {
        'status': 'success',
        'rows': len(df),
        's3_path': f"s3://{bucket}/{s3_path}",
        'ds': ds,
    }


extract_task = PythonOperator(
    task_id='extract_events_to_s3',
    python_callable=extract_events_to_s3,
    provide_context=True,
    dag=dag,
)

extract_task
