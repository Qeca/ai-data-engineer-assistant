from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'rfm_features_daily',
    default_args=default_args,
    description='Ежедневный расчет RFM-фичей для клиентов',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['rfm', 'features', 'daily'],
)

spark_submit = SparkSubmitOperator(
    task_id='run_rfm_features',
    application='/workspace/infra/spark/jobs/rfm_features.py',
    name='rfm_features_daily',
    conn_id='spark_default',
    verbose=True,
    dag=dag,
)

spark_submit