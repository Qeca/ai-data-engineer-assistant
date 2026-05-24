from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='daily_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline with decorators',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'daily'],
)
def daily_etl_pipeline():
    
    @task
    def extract_data():
        """Extract data from source"""
        logging.info("Starting data extraction")
        data = {
            'records': [
                {'id': 1, 'value': 100},
                {'id': 2, 'value': 200},
                {'id': 3, 'value': 300},
            ],
            'extracted_at': datetime.now().isoformat()
        }
        logging.info(f"Extracted {len(data['records'])} records")
        return data
    
    @task
    def transform_data(data):
        """Transform extracted data"""
        logging.info("Starting data transformation")
        transformed = []
        for record in data['records']:
            transformed_record = {
                'id': record['id'],
                'value': record['value'] * 1.1,  # Apply 10% increase
                'processed_at': datetime.now().isoformat()
            }
            transformed.append(transformed_record)
        logging.info(f"Transformed {len(transformed)} records")
        return {
            'records': transformed,
            'processed_at': data['extracted_at']
        }
    
    @task
    def load_data(data):
        """Load transformed data to destination"""
        logging.info("Starting data load")
        for record in data['records']:
            logging.info(f"Loading record: {record}")
        logging.info(f"Loaded {len(data['records'])} records successfully")
        return {
            'status': 'success',
            'loaded_count': len(data['records']),
            'completed_at': datetime.now().isoformat()
        }
    
    @task
    def send_notification(result):
        """Send notification about pipeline completion"""
        logging.info(f"Pipeline completed: {result['status']}")
        logging.info(f"Loaded {result['loaded_count']} records")
        return result
    
    # Define task dependencies
    extracted = extract_data()
    transformed = transform_data(extracted)
    loaded = load_data(transformed)
    notified = send_notification(loaded)
    
    return notified

# Instantiate the DAG
dag = daily_etl_pipeline()