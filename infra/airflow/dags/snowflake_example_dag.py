from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeOperator,
    SnowflakeCheckOperator,
    SnowflakeIntervalCheckOperator,
    SnowflakeSqlApiOperator,
    SnowflakeValueCheckOperator,
)
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'snowflake_example_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['snowflake', 'example'],
)

# Task 1: Bash operator for initial setup
setup_task = BashOperator(
    task_id='setup_environment',
    bash_command='echo "Setting up Snowflake environment"',
    dag=dag,
)

# Task 2: Python operator for data preparation
prepare_data = PythonOperator(
    task_id='prepare_data',
    python_callable=lambda: print("Preparing data for Snowflake"),
    dag=dag,
)

# Task 3: SQLExecuteQueryOperator (SnowflakeOperator) - Execute DDL
create_table = SnowflakeOperator(
    task_id='create_table',
    sql="""
    CREATE TABLE IF NOT EXISTS example_table (
        id INTEGER,
        name VARCHAR(100),
        created_at TIMESTAMP
    )
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task 4: SQLExecuteQueryOperator (SnowflakeOperator) - Execute DML
insert_data = SnowflakeOperator(
    task_id='insert_data',
    sql="""
    INSERT INTO example_table (id, name, created_at)
    VALUES (1, 'Test Record', CURRENT_TIMESTAMP())
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task 5: SnowflakeCheckOperator - Check if table exists
check_table_exists = SnowflakeCheckOperator(
    task_id='check_table_exists',
    sql="SELECT COUNT(*) FROM example_table WHERE id = 1",
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task 6: SnowflakeValueCheckOperator - Check specific value
check_value = SnowflakeValueCheckOperator(
    task_id='check_value',
    sql="SELECT COUNT(*) FROM example_table",
    pass_value='1',
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task 7: SnowflakeIntervalCheckOperator - Check data freshness
check_interval = SnowflakeIntervalCheckOperator(
    task_id='check_interval',
    table='example_table',
    metrics_thresholds={'COUNT(*)': {'lower_bound': 1, 'upper_bound': 100}},
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task 8: SnowflakeSqlApiOperator - Async SQL execution
async_query = SnowflakeSqlApiOperator(
    task_id='async_query',
    sql="SELECT * FROM example_table LIMIT 10",
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task 9: SQLExecuteQueryOperator - Update data
update_data = SnowflakeOperator(
    task_id='update_data',
    sql="""
    UPDATE example_table 
    SET name = 'Updated Record' 
    WHERE id = 1
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task 10: SnowflakeCheckOperator - Verify update
verify_update = SnowflakeCheckOperator(
    task_id='verify_update',
    sql="SELECT COUNT(*) FROM example_table WHERE name = 'Updated Record'",
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task 11: Bash operator for cleanup notification
cleanup_notification = BashOperator(
    task_id='cleanup_notification',
    bash_command='echo "Snowflake DAG execution completed successfully"',
    dag=dag,
)

# Define task dependencies
setup_task >> prepare_data >> create_table >> insert_data >> check_table_exists
check_table_exists >> check_value >> check_interval
check_interval >> async_query >> update_data >> verify_update
verify_update >> cleanup_notification
