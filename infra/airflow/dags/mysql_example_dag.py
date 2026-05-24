from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'mysql_example_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['mysql', 'example'],
)

# Задача 1: Создание таблицы
create_table_task = MySQLExecuteQueryOperator(
    task_id='create_table',
    mysql_conn_id='mysql_default',
    sql="""
        CREATE TABLE IF NOT EXISTS example_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(100) NOT NULL,
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    dag=dag,
)

# Задача 2: Вставка данных
insert_data_task = MySQLExecuteQueryOperator(
    task_id='insert_data',
    mysql_conn_id='mysql_default',
    sql="""
        INSERT INTO example_users (username, email)
        VALUES ('user1', 'user1@example.com'),
               ('user2', 'user2@example.com')
    """,
    dag=dag,
)

# Определение зависимостей
create_table_task >> insert_data_task