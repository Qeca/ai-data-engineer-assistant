from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksCreateJobsOperator,
    DatabricksNotebookOperator,
    DatabricksRunNowOperator,
    DatabricksSQLStatementsOperator,
    DatabricksSubmitRunOperator,
    DatabricksTaskOperator,
)
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'databricks_example_dag',
    default_args=default_args,
    description='DAG with 10 Databricks tasks',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['databricks', 'example'],
)

# Task 1: Create a Databricks job
task1 = DatabricksCreateJobsOperator(
    task_id='create_databricks_job',
    json={
        'name': 'example_job',
        'new_cluster': {
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'Standard_DS3_v2',
            'num_workers': 2,
        },
        'notebook_task': {
            'notebook_path': '/test',
        },
    },
    databricks_conn_id='databricks_default',
    dag=dag,
)

# Task 2: Run a notebook
task2 = DatabricksNotebookOperator(
    task_id='run_notebook',
    notebook='/test',
    new_cluster={
        'spark_version': '13.3.x-scala2.12',
        'node_type_id': 'Standard_DS3_v2',
        'num_workers': 2,
    },
    databricks_conn_id='databricks_default',
    dag=dag,
)

# Task 3: Run now an existing job
task3 = DatabricksRunNowOperator(
    task_id='run_now_job',
    job_id=123,
    databricks_conn_id='databricks_default',
    dag=dag,
)

# Task 4: Execute SQL statements
task4 = DatabricksSQLStatementsOperator(
    task_id='execute_sql',
    sql='SELECT * FROM example_table LIMIT 100',
    warehouse_id='example_warehouse_id',
    databricks_conn_id='databricks_default',
    dag=dag,
)

# Task 5: Submit a run
task5 = DatabricksSubmitRunOperator(
    task_id='submit_run',
    json={
        'new_cluster': {
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'Standard_DS3_v2',
            'num_workers': 2,
        },
        'notebook_task': {
            'notebook_path': '/Users/example@example.com/notebook',
        },
    },
    databricks_conn_id='databricks_default',
    dag=dag,
)

# Task 6: Databricks task operator
task6 = DatabricksTaskOperator(
    task_id='databricks_task',
    json={
        'task_key': 'example_task',
        'description': 'Example Databricks task',
        'existing_cluster_id': 'example_cluster_id',
        'spark_jar_task': {
            'main_class_name': 'com.example.Main',
        },
    },
    databricks_conn_id='databricks_default',
    dag=dag,
)

# Task 7: Another notebook task
task7 = DatabricksNotebookOperator(
    task_id='run_notebook_2',
    notebook='/Shared/etl/process_data',
    new_cluster={
        'spark_version': '13.3.x-scala2.12',
        'node_type_id': 'Standard_DS3_v2',
        'num_workers': 4,
    },
    databricks_conn_id='databricks_default',
    dag=dag,
)

# Task 8: Another SQL statement
task8 = DatabricksSQLStatementsOperator(
    task_id='execute_sql_2',
    sql='INSERT INTO target_table SELECT * FROM source_table WHERE date = current_date()',
    warehouse_id='example_warehouse_id',
    databricks_conn_id='databricks_default',
    dag=dag,
)

# Task 9: Submit another run with JAR
task9 = DatabricksSubmitRunOperator(
    task_id='submit_jar_run',
    json={
        'new_cluster': {
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'Standard_DS3_v2',
            'num_workers': 2,
        },
        'spark_jar_task': {
            'main_class_name': 'com.example.ETLJob',
            'parameters': ['--input', '/data/input', '--output', '/data/output'],
        },
        'libraries': [
            {'jar': 'dbfs:/mnt/libs/example.jar'}
        ],
    },
    databricks_conn_id='databricks_default',
    dag=dag,
)

# Task 10: Create another job
task10 = DatabricksCreateJobsOperator(
    task_id='create_databricks_job_2',
    json={
        'name': 'example_job_2',
        'new_cluster': {
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'Standard_DS3_v2',
            'num_workers': 3,
        },
        'spark_jar_task': {
            'main_class_name': 'com.example.BatchJob',
        },
    },
    databricks_conn_id='databricks_default',
    dag=dag,
)

# Define task dependencies (sequential execution)
task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8 >> task9 >> task10