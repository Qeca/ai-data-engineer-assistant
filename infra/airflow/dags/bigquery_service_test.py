from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryUpdateDatasetOperator,
)
from datetime import datetime
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'bigquery_service_test',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['bigquery', 'testing'],
)

# Task 1: Start marker
start = DummyOperator(task_id='start', dag=dag)

# Task 2: Create empty dataset
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset',
    dataset_id='test_dataset_bq',
    project_id='my-gcp-project',
    location='US',
    dag=dag,
)

# Task 3: Update dataset (add description)
update_dataset = BigQueryUpdateDatasetOperator(
    task_id='update_dataset',
    dataset_id='test_dataset_bq',
    project_id='my-gcp-project',
    dataset_resource={'description': 'Test dataset for BigQuery operators'},
    fields=['description'],
    dag=dag,
)

# Task 4: Create table 1
create_table_1 = BigQueryCreateTableOperator(
    task_id='create_table_1',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='users_table',
    schema_fields=[
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'username', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    ],
    dag=dag,
)

# Task 5: Create table 2
create_table_2 = BigQueryCreateTableOperator(
    task_id='create_table_2',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='orders_table',
    schema_fields=[
        {'name': 'order_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'order_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'total_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    dag=dag,
)

# Task 6: Create table 3
create_table_3 = BigQueryCreateTableOperator(
    task_id='create_table_3',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='products_table',
    schema_fields=[
        {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'product_name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'price', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    dag=dag,
)

# Task 7: Create table 4
create_table_4 = BigQueryCreateTableOperator(
    task_id='create_table_4',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='inventory_table',
    schema_fields=[
        {'name': 'inventory_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'quantity', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'warehouse', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    dag=dag,
)

# Task 8: Create table 5
create_table_5 = BigQueryCreateTableOperator(
    task_id='create_table_5',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='transactions_table',
    schema_fields=[
        {'name': 'transaction_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'order_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'payment_method', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
    ],
    dag=dag,
)

# Task 9: Get dataset tables (list all tables)
get_tables = BigQueryGetDatasetTablesOperator(
    task_id='get_dataset_tables',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    dag=dag,
)

# Task 10: Update dataset again (add labels)
update_dataset_labels = BigQueryUpdateDatasetOperator(
    task_id='update_dataset_labels',
    dataset_id='test_dataset_bq',
    project_id='my-gcp-project',
    dataset_resource={'labels': {'environment': 'testing', 'team': 'data-engineering'}},
    fields=['labels'],
    dag=dag,
)

# Task 11: Create table 6 (logs)
create_table_6 = BigQueryCreateTableOperator(
    task_id='create_table_6',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='logs_table',
    schema_fields=[
        {'name': 'log_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'level', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'message', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    dag=dag,
)

# Task 12: Create table 7 (metrics)
create_table_7 = BigQueryCreateTableOperator(
    task_id='create_table_7',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='metrics_table',
    schema_fields=[
        {'name': 'metric_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'metric_name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'value', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'recorded_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    ],
    dag=dag,
)

# Task 13: Get dataset tables again (verify)
get_tables_verify = BigQueryGetDatasetTablesOperator(
    task_id='get_dataset_tables_verify',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    dag=dag,
)

# Task 14: Delete table 1 (users_table)
delete_table_1 = BigQueryDeleteTableOperator(
    task_id='delete_table_1',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='users_table',
    dag=dag,
)

# Task 15: Delete table 2 (orders_table)
delete_table_2 = BigQueryDeleteTableOperator(
    task_id='delete_table_2',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='orders_table',
    dag=dag,
)

# Task 16: Delete table 3 (products_table)
delete_table_3 = BigQueryDeleteTableOperator(
    task_id='delete_table_3',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='products_table',
    dag=dag,
)

# Task 17: Delete table 4 (inventory_table)
delete_table_4 = BigQueryDeleteTableOperator(
    task_id='delete_table_4',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='inventory_table',
    dag=dag,
)

# Task 18: Delete table 5 (transactions_table)
delete_table_5 = BigQueryDeleteTableOperator(
    task_id='delete_table_5',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    table_id='transactions_table',
    dag=dag,
)

# Task 19: Get dataset tables (final check)
get_tables_final = BigQueryGetDatasetTablesOperator(
    task_id='get_dataset_tables_final',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    dag=dag,
)

# Task 20: Delete dataset
delete_dataset = BigQueryDeleteDatasetOperator(
    task_id='delete_dataset',
    project_id='my-gcp-project',
    dataset_id='test_dataset_bq',
    delete_contents=True,
    dag=dag,
)

# Define dependencies
start >> create_dataset >> update_dataset >> create_table_1 >> create_table_2 >> create_table_3 >> create_table_4 >> create_table_5 >> get_tables >> update_dataset_labels >> create_table_6 >> create_table_7 >> get_tables_verify >> delete_table_1 >> delete_table_2 >> delete_table_3 >> delete_table_4 >> delete_table_5 >> get_tables_final >> delete_dataset
