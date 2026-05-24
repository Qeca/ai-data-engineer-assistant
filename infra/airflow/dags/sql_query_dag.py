from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='sql_query_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['sql', 'demo'],
) as dag:

    task_1 = SQLExecuteQueryOperator(
        task_id='query_1',
        sql='SELECT 1 AS id, \'task_1\' AS name',
        conn_id='demo-postgres-warehouse',
    )

    task_2 = SQLExecuteQueryOperator(
        task_id='query_2',
        sql='SELECT 2 AS id, \'task_2\' AS name',
        conn_id='demo-postgres-warehouse',
    )

    task_3 = SQLExecuteQueryOperator(
        task_id='query_3',
        sql='SELECT 3 AS id, \'task_3\' AS name',
        conn_id='demo-postgres-warehouse',
    )

    task_4 = SQLExecuteQueryOperator(
        task_id='query_4',
        sql='SELECT 4 AS id, \'task_4\' AS name',
        conn_id='demo-postgres-warehouse',
    )

    task_1 >> task_2 >> task_3 >> task_4
