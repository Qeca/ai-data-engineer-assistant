from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="clickstream_aggregation",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "analytics", "retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    aggregate = BashOperator(
        task_id="aggregate_clickstream",
        bash_command="python /opt/airflow/jobs/sample_job.py",
    )

    aggregate
