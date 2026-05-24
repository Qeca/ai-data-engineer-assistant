from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryColumnCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteDatasetOperator,
)
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'bigquery_example_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['bigquery', 'example', 'gcp'],
)

# Task 1: Start logging
start_logging = BashOperator(
    task_id='start_logging',
    bash_command='echo "Starting BigQuery DAG execution at $(date)"',
    dag=dag,
)

# Task 2: Create empty dataset
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset',
    dataset_id='example_dataset',
    project_id='my-gcp-project',
    location='US',
    dag=dag,
)

# Task 3: Log dataset creation
log_dataset_creation = BashOperator(
    task_id='log_dataset_creation',
    bash_command='echo "Dataset example_dataset created successfully"',
    dag=dag,
)

# Task 4: Create table 1 - users
create_table_users = BigQueryCreateTableOperator(
    task_id='create_table_users',
    table_resource={
        'tableReference': {
            'projectId': 'my-gcp-project',
            'datasetId': 'example_dataset',
            'tableId': 'users',
        },
        'schema': {
            'fields': [
                {'name': 'user_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'username', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            ]
        },
    },
    dag=dag,
)

# Task 5: Create table 2 - orders
create_table_orders = BigQueryCreateTableOperator(
    task_id='create_table_orders',
    table_resource={
        'tableReference': {
            'projectId': 'my-gcp-project',
            'datasetId': 'example_dataset',
            'tableId': 'orders',
        },
        'schema': {
            'fields': [
                {'name': 'order_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'user_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'order_date', 'type': 'DATE', 'mode': 'REQUIRED'},
                {'name': 'total_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        },
    },
    dag=dag,
)

# Task 6: Create table 3 - products
create_table_products = BigQueryCreateTableOperator(
    task_id='create_table_products',
    table_resource={
        'tableReference': {
            'projectId': 'my-gcp-project',
            'datasetId': 'example_dataset',
            'tableId': 'products',
        },
        'schema': {
            'fields': [
                {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'product_name', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'price', 'type': 'FLOAT', 'mode': 'REQUIRED'},
                {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        },
    },
    dag=dag,
)

# Task 7: Log table creation
log_table_creation = BashOperator(
    task_id='log_table_creation',
    bash_command='echo "All tables created successfully"',
    dag=dag,
)

# Task 8: Check dataset exists
check_dataset_exists = BigQueryCheckOperator(
    task_id='check_dataset_exists',
    sql="SELECT COUNT(*) FROM `my-gcp-project.example_dataset.INFORMATION_SCHEMA.TABLES`",
    dag=dag,
)

# Task 9: Check users table schema
check_users_table = BigQueryColumnCheckOperator(
    task_id='check_users_table',
    sql="SELECT user_id, username, email FROM `my-gcp-project.example_dataset.users` LIMIT 1",
    column_mapping={
        'user_id': {'check': 'not_null'},
        'username': {'check': 'not_null'},
    },
    dag=dag,
)

# Task 10: Check orders table schema
check_orders_table = BigQueryColumnCheckOperator(
    task_id='check_orders_table',
    sql="SELECT order_id, user_id, order_date FROM `my-gcp-project.example_dataset.orders` LIMIT 1",
    column_mapping={
        'order_id': {'check': 'not_null'},
        'user_id': {'check': 'not_null'},
    },
    dag=dag,
)

# Task 11: Check products table schema
check_products_table = BigQueryColumnCheckOperator(
    task_id='check_products_table',
    sql="SELECT product_id, product_name, price FROM `my-gcp-project.example_dataset.products` LIMIT 1",
    column_mapping={
        'product_id': {'check': 'not_null'},
        'product_name': {'check': 'not_null'},
    },
    dag=dag,
)

# Task 12: Validate table count
validate_table_count = BigQueryCheckOperator(
    task_id='validate_table_count',
    sql="SELECT COUNT(*) >= 3 FROM `my-gcp-project.example_dataset.INFORMATION_SCHEMA.TABLES`",
    dag=dag,
)

# Task 13: Log validation results
log_validation = BashOperator(
    task_id='log_validation',
    bash_command='echo "All validations passed successfully"',
    dag=dag,
)

# Task 14: Check data quality
check_data_quality = BigQueryCheckOperator(
    task_id='check_data_quality',
    sql="SELECT COUNT(*) FROM `my-gcp-project.example_dataset.users` WHERE username IS NOT NULL",
    dag=dag,
)

# Task 15: Log before cleanup
log_before_cleanup = BashOperator(
    task_id='log_before_cleanup',
    bash_command='echo "Starting cleanup process"',
    dag=dag,
)

# Task 16: Delete dataset (with all tables)
delete_dataset = BigQueryDeleteDatasetOperator(
    task_id='delete_dataset',
    dataset_id='example_dataset',
    project_id='my-gcp-project',
    delete_contents=True,
    dag=dag,
)

# Task 17: End logging
end_logging = BashOperator(
    task_id='end_logging',
    bash_command='echo "BigQuery DAG execution completed at $(date)"',
    dag=dag,
)

# Define task dependencies
start_logging >> create_dataset >> log_dataset_creation
log_dataset_creation >> create_table_users >> create_table_orders >> create_table_products
create_table_products >> log_table_creation
log_table_creation >> check_dataset_exists
check_dataset_exists >> check_users_table >> check_orders_table >> check_products_table
check_products_table >> validate_table_count
validate_table_count >> log_validation
log_validation >> check_data_quality
check_data_quality >> log_before_cleanup
log_before_cleanup >> delete_dataset
delete_dataset >> end_logging
