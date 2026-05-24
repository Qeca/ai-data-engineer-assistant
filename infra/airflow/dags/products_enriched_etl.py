from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# API endpoint для submit_spark_job
SPARK_API_URL = "http://localhost:8080/api/v1/spark/submit"

def extract_products(**context):
    """Извлечение данных из MySQL retail.products и сохранение в staging/products.parquet через Spark"""
    print("Starting extract task...")
    
    # Spark скрипт для извлечения
    spark_code = '''
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("retail_products_extract").getOrCreate()

# Чтение из MySQL retail.products
df = spark.read.format("jdbc") \\
    .option("url", "jdbc:mysql://demo-mysql:3306/retail_db") \\
    .option("dbtable", "products") \\
    .option("user", "demo") \\
    .option("password", "demo") \\
    .option("driver", "com.mysql.cj.jdbc.Driver") \\
    .load()

# Сохранение в parquet
df.write.mode("overwrite").parquet("/workspace/staging/products.parquet")
count = df.count()
print(f"Extracted {count} rows to staging/products.parquet")
spark.stop()
'''
    
    # Отправляем запрос на запуск Spark job
    payload = {
        "name": "extract_products",
        "app_resource": spark_code,
        "executor_memory": "1g",
        "partitions": 4
    }
    
    try:
        response = requests.post(SPARK_API_URL, json=payload, timeout=300)
        result = response.json()
        print(f"Spark job submitted: {result}")
        return {'extracted': True, 'job_result': result, 'timestamp': datetime.now().isoformat()}
    except Exception as e:
        print(f"Error submitting Spark job: {e}")
        raise

def transform_products(**context):
    """Трансформация: добавление колонки margin_pct через Spark"""
    print("Starting transform task...")
    
    spark_code = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as sql_round

spark = SparkSession.builder.appName("retail_products_transform").getOrCreate()

# Чтение из parquet
df = spark.read.parquet("/workspace/staging/products.parquet")

# Добавление колонки margin_pct = ((price - cost) / price) * 100
df_transformed = df.withColumn(
    "margin_pct",
    sql_round(((col("price") - col("cost")) / col("price")) * 100, 2)
)

# Сохранение результата
df_transformed.write.mode("overwrite").parquet("/workspace/staging/products_enriched.parquet")
count = df_transformed.count()
print(f"Transformed {count} rows with margin_pct")
spark.stop()
'''
    
    payload = {
        "name": "transform_products",
        "app_resource": spark_code,
        "executor_memory": "1g",
        "partitions": 4
    }
    
    try:
        response = requests.post(SPARK_API_URL, json=payload, timeout=300)
        result = response.json()
        print(f"Spark job submitted: {result}")
        return {'transformed': True, 'job_result': result, 'timestamp': datetime.now().isoformat()}
    except Exception as e:
        print(f"Error submitting Spark job: {e}")
        raise

def load_products(**context):
    """Загрузка результата в Postgres analytics.products_enriched"""
    print("Starting load task...")
    
    spark_code = '''
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("retail_products_load").getOrCreate()

# Чтение трансформированных данных
df = spark.read.parquet("/workspace/staging/products_enriched.parquet")

# Загрузка в Postgres через JDBC
df.write.format("jdbc") \\
    .option("url", "jdbc:postgresql://demo-postgres:5432/analytics") \\
    .option("dbtable", "analytics.products_enriched") \\
    .option("user", "demo") \\
    .option("password", "demo") \\
    .option("driver", "org.postgresql.Driver") \\
    .mode("overwrite") \\
    .save()

count = df.count()
print(f"Loaded {count} rows to analytics.products_enriched")
spark.stop()
'''
    
    payload = {
        "name": "load_products",
        "app_resource": spark_code,
        "executor_memory": "1g",
        "partitions": 4
    }
    
    try:
        response = requests.post(SPARK_API_URL, json=payload, timeout=300)
        result = response.json()
        print(f"Spark job submitted: {result}")
        return {'loaded': True, 'job_result': result, 'timestamp': datetime.now().isoformat()}
    except Exception as e:
        print(f"Error submitting Spark job: {e}")
        raise

dag = DAG(
    'products_enriched_etl',
    default_args=default_args,
    description='ETL пайплайн для продуктов: extract из MySQL retail.products, transform с margin_pct, load в Postgres analytics.products_enriched',
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'etl', 'products'],
)

start = EmptyOperator(task_id='start', dag=dag)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_products,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_products,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_products,
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

# Зависимости задач: extract >> transform >> load
start >> extract >> transform >> load >> end
