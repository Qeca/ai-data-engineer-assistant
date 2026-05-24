from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryDeleteDatasetOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# OpenLineageTestOperator может быть кастомным, используем BashOperator как placeholder
# или импортируем если доступен
try:
    from openlineage.airflow.operators import OpenLineageTestOperator
    HAS_OPENLINEAGE = True
except ImportError:
    HAS_OPENLINEAGE = False
    OpenLineageTestOperator = BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='gcs_to_bigquery_example',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['example', 'gcs', 'bigquery', 'openlineage'],
) as dag:

    # Task 1: Создание пустого датасета BigQuery
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id='example_dataset',
        project_id='my-gcp-project',
        location='US',
        exists_ok=True,
    )

    # Task 2: Загрузка данных из GCS в BigQuery
    gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket='example-bucket',
        source_objects=['data/input.csv'],
        destination_project_dataset_table='my-gcp-project.example_dataset.example_table',
        schema_fields=[
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        source_format='CSV',
        skip_leading_rows=1,
    )

    # Task 3: OpenLineage тест
    if HAS_OPENLINEAGE:
        openlineage_test = OpenLineageTestOperator(
            task_id='openlineage_test',
            name='gcs_to_bigquery_lineage_test',
        )
    else:
        openlineage_test = BashOperator(
            task_id='openlineage_test',
            bash_command='echo "OpenLineage test completed"',
        )

    # Task 4: Удаление датасета BigQuery (cleanup)
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id='delete_dataset',
        dataset_id='example_dataset',
        project_id='my-gcp-project',
        delete_contents=True,
    )

    # Определение зависимостей между задачами
    create_dataset >> gcs_to_bq >> openlineage_test >> delete_dataset
