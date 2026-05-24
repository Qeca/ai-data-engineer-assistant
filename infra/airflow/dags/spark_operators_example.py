from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.pyspark import PySparkOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'spark_operators_example',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'example'],
)

# Задача 1: PySparkOperator - запуск PySpark скрипта
pyspark_task = PySparkOperator(
    task_id='run_pyspark_script',
    py_files=['/opt/spark/scripts/sample_pyspark.py'],
    application_args=['--input', '/data/input', '--output', '/data/output'],
    conf={
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
    },
    dag=dag,
)

# Задача 2: SparkSubmitOperator - отправка Spark приложения
spark_submit_task = SparkSubmitOperator(
    task_id='submit_spark_app',
    application='/opt/spark/apps/sample_app.jar',
    conf={
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g',
    },
    application_args=['--mode', 'cluster'],
    dag=dag,
)

# Задача 3: SparkSqlOperator - выполнение Spark SQL запроса
spark_sql_task = SparkSqlOperator(
    task_id='run_spark_sql',
    sql='SELECT COUNT(*) FROM sample_table WHERE date >= CURRENT_DATE - INTERVAL 7 DAYS',
    dag=dag,
)

# Задача 4: SparkJDBCOperator - работа с JDBC источником
spark_jdbc_task = SparkJDBCOperator(
    task_id='jdbc_read',
    url='jdbc:postgresql://localhost:5432/sample_db',
    table='sample_table',
    driver='org.postgresql.Driver',
    dag=dag,
)

# Задача 5: SparkSubmitOperator - вторая задача отправки
spark_submit_task_2 = SparkSubmitOperator(
    task_id='submit_etl_job',
    application='/opt/spark/apps/etl_job.py',
    conf={
        'spark.executor.memory': '2g',
        'spark.sql.shuffle.partitions': '200',
    },
    application_args=['--env', 'production'],
    dag=dag,
)

# Определение зависимостей между задачами
pyspark_task >> spark_submit_task >> spark_sql_task >> spark_jdbc_task >> spark_submit_task_2
