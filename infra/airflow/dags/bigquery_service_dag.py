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
    'bigquery_service_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['bigquery', 'gcp', 'example'],
)

# Task 1: Start notification
start_task = BashOperator(
    task_id='start_notification',
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
                {'name': 'amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
                {'name': 'order_date', 'type': 'DATE', 'mode': 'NULLABLE'},
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
check_dataset = BigQueryCheckOperator(
    task_id='check_dataset_exists',
    sql='SELECT COUNT(*) FROM `my-gcp-project.example_dataset.INFORMATION_SCHEMA.TABLES`',
    dag=dag,
)

# Task 9: Check users table schema
check_users_columns = BigQueryColumnCheckOperator(
    task_id='check_users_columns',
    sql='SELECT user_id, username, email, created_at FROM `my-gcp-project.example_dataset.users` LIMIT 0',
    column_mapping={
        'user_id': {'check': 'not_null'},
        'username': {'check': 'not_null'},
    },
    dag=dag,
)

# Task 10: Check orders table schema
check_orders_columns = BigQueryColumnCheckOperator(
    task_id='check_orders_columns',
    sql='SELECT order_id, user_id, amount, order_date FROM `my-gcp-project.example_dataset.orders` LIMIT 0',
    column_mapping={
        'order_id': {'check': 'not_null'},
        'amount': {'check': 'not_null'},
    },
    dag=dag,
)

# Task 11: Check products table schema
check_products_columns = BigQueryColumnCheckOperator(
    task_id='check_products_columns',
    sql='SELECT product_id, product_name, price, category FROM `my-gcp-project.example_dataset.products` LIMIT 0',
    column_mapping={
        'product_id': {'check': 'not_null'},
        'price': {'check': 'not_null'},
    },
    dag=dag,
)

# Task 12: Validate table count
validate_table_count = BigQueryCheckOperator(
    task_id='validate_table_count',
    sql='SELECT COUNT(*) >= 3 FROM `my-gcp-project.example_dataset.INFORMATION_SCHEMA.TABLES`',
    dag=dag,
)

# Task 13: Log validation results
log_validation = BashOperator(
    task_id='log_validation',
    bash_command='echo "All schema validations passed successfully"',
    dag=dag,
)

# Task 14: Generate summary report
generate_report = BashOperator(
    task_id='generate_summary_report',
    bash_command='''
    echo "=== BigQuery DAG Summary Report ==="
    echo "Dataset: example_dataset"
    echo "Tables: users, orders, products"
    echo "Execution Time: $(date)"
    echo "Status: SUCCESS"
    ''',
    dag=dag,
)

# Task 15: Send completion notification
completion_notification = BashOperator(
    task_id='completion_notification',
    bash_command='echo "BigQuery DAG completed successfully at $(date)"',
    dag=dag,
)

# Task 16: Log cleanup warning
cleanup_warning = BashOperator(
    task_id='cleanup_warning',
    bash_command='echo "WARNING: About to delete dataset example_dataset"',
    dag=dag,
)

# Task 17: Delete dataset
delete_dataset = BigQueryDeleteDatasetOperator(
    task_id='delete_dataset',
    dataset_id='example_dataset',
    project_id='my-gcp-project',
    delete_contents=True,
    dag=dag,
)

# Define task dependencies
start_task >> create_dataset >> log_dataset_creation
log_dataset_creation >> create_table_users >> create_table_orders >> create_table_products
create_table_products >> log_table_creation
log_table_creation >> check_dataset
check_dataset >> check_users_columns >> check_orders_columns >> check_products_columns
check_products_columns >> validate_table_count
validate_table_count >> log_validation
log_validation >> generate_report
generate_report >> completion_notification
completion_notification >> cleanup_warning
cleanup_warning >> delete_dataset
