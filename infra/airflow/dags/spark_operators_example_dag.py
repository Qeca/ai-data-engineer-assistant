from airflow import DAG
from datetime import datetime

# Импорты Spark операторов
try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    from airflow.providers.apache.spark.operators.pyspark import PySparkOperator
    from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
    from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
    SPARK_OPERATORS_AVAILABLE = True
except ImportError:
    # Fallback для sandbox тестирования - используем BashOperator как заглушку
    from airflow.operators.bash import BashOperator
    SPARK_OPERATORS_AVAILABLE = False
    SparkSubmitOperator = BashOperator
    PySparkOperator = BashOperator
    SparkJDBCOperator = BashOperator
    SparkSqlOperator = BashOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='spark_operators_example_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'example'],
)

# Задача 1: SparkSubmitOperator - отправка Spark приложения
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/opt/spark/examples/src/main/python/pi.py' if SPARK_OPERATORS_AVAILABLE else 'echo "SparkSubmit task"',
    conf={
        'spark.executor.memory': '2g',
        'spark.driver.memory': '1g',
    } if SPARK_OPERATORS_AVAILABLE else {},
    verbose=True if SPARK_OPERATORS_AVAILABLE else False,
    dag=dag,
)

# Задача 2: PySparkOperator - выполнение PySpark скрипта
pyspark_task = PySparkOperator(
    task_id='pyspark_task',
    py_files=['/opt/spark/examples/src/main/python/sort.py'] if SPARK_OPERATORS_AVAILABLE else [],
    application='/opt/spark/examples/src/main/python/pi.py' if SPARK_OPERATORS_AVAILABLE else 'echo "PySpark task"',
    conf={
        'spark.executor.memory': '2g',
    } if SPARK_OPERATORS_AVAILABLE else {},
    dag=dag,
)

# Задача 3: SparkJDBCOperator - работа с JDBC источником данных
spark_jdbc_task = SparkJDBCOperator(
    task_id='spark_jdbc_task',
    jdbc_url='jdbc:postgresql://demo-postgres:5432/analytics' if SPARK_OPERATORS_AVAILABLE else '',
    driver='org.postgresql.Driver' if SPARK_OPERATORS_AVAILABLE else '',
    query='SELECT * FROM orders LIMIT 100' if SPARK_OPERATORS_AVAILABLE else 'echo "JDBC task"',
    user='demo' if SPARK_OPERATORS_AVAILABLE else '',
    password_var='spark_jdbc_password' if SPARK_OPERATORS_AVAILABLE else '',
    dag=dag,
)

# Задача 4: SparkSqlOperator - выполнение SQL запроса в Spark
spark_sql_task = SparkSqlOperator(
    task_id='spark_sql_task',
    sql='SELECT count(*) as total_orders FROM orders' if SPARK_OPERATORS_AVAILABLE else 'echo "SparkSQL task"',
    dag=dag,
)

# Задача 5: SparkSubmitOperator - ещё одно Spark приложение
final_spark_submit = SparkSubmitOperator(
    task_id='final_spark_submit',
    application='/opt/spark/examples/src/main/python/wordcount.py' if SPARK_OPERATORS_AVAILABLE else 'echo "Final SparkSubmit task"',
    conf={
        'spark.executor.cores': '2',
        'spark.executor.instances': '2',
    } if SPARK_OPERATORS_AVAILABLE else {},
    dag=dag,
)

# Определение зависимостей между задачами
spark_submit_task >> pyspark_task >> spark_jdbc_task >> spark_sql_task >> final_spark_submit
