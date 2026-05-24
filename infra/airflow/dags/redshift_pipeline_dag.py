from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftCreateClusterOperator, RedshiftDeleteClusterOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'redshift_pipeline_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['redshift', 'aws'],
)

# Задача 1: Создание кластера Redshift
create_cluster = RedshiftCreateClusterOperator(
    task_id='create_redshift_cluster',
    cluster_identifier='redshift-cluster-{{ ds_nodash }}',
    node_type='dc2.large',
    number_of_nodes=2,
    master_username='admin',
    master_user_password='{{ var.value.redshift_password }}',
    db_name='dev',
    cluster_type='multi-node',
    dag=dag,
)

# Задача 2: Проверка таблицы после создания
check_table_exists = SQLTableCheckOperator(
    task_id='check_table_exists',
    table='public.sample_table',
    checks={
        'row_count': {'check_statement': 'COUNT(*) >= 0'},
    },
    dag=dag,
)

# Задача 3: Выполнение SQL запроса через Redshift Data API
run_query_1 = RedshiftDataOperator(
    task_id='run_etl_query',
    database='dev',
    cluster_identifier='redshift-cluster-{{ ds_nodash }}',
    sql="""
        CREATE TABLE IF NOT EXISTS public.sample_table (
            id INTEGER,
            name VARCHAR(100),
            created_at TIMESTAMP
        );
    """,
    dag=dag,
)

# Задача 4: Вставка данных через Redshift Data API
insert_data = RedshiftDataOperator(
    task_id='insert_sample_data',
    database='dev',
    cluster_identifier='redshift-cluster-{{ ds_nodash }}',
    sql="""
        INSERT INTO public.sample_table (id, name, created_at)
        VALUES (1, 'Test Record', GETDATE());
    """,
    dag=dag,
)

# Задача 5: Проверка данных в таблице
validate_data = SQLTableCheckOperator(
    task_id='validate_data_count',
    table='public.sample_table',
    checks={
        'row_count': {'check_statement': 'COUNT(*) >= 1'},
    },
    dag=dag,
)

# Задача 6: Удаление кластера Redshift
delete_cluster = RedshiftDeleteClusterOperator(
    task_id='delete_redshift_cluster',
    cluster_identifier='redshift-cluster-{{ ds_nodash }}',
    skip_final_cluster_snapshot=True,
    dag=dag,
)

# Определение зависимостей
create_cluster >> check_table_exists >> run_query_1 >> insert_data >> validate_data >> delete_cluster
