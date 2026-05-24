from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="rfm_features_dag",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "data_engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Daily RFM features calculation for customer segmentation",
    tags=["features", "rfm", "spark"],
) as dag:
    calculate_rfm = BashOperator(
        task_id="calculate_rfm_features",
        bash_command="spark-submit --name rfm_features_calculation /workspace/infra/spark/jobs/rfm_features.py",
    )

    calculate_rfm
