from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import subprocess
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

def extract_products(**context):
    """Извлечение данных из MySQL retail.products и сохранение в staging/products.parquet через Spark"""
    print("Starting extract task...")
    
    # Spark скрипт для извлечения данных из MySQL
    spark_code = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
print(f"Extracted {df.count()} rows to staging/products.parquet")
spark.stop()
'''
    
    # Сохраняем скрипт во временный файл
    with open('/tmp/extract_products.py', 'w') as f:
        f.write(spark_code)
    
    # Запускаем Spark через submit_spark_job API (симуляция через subprocess)
    result = subprocess.run(
        ['spark-submit', '--master', 'local', '/tmp/extract_products.py'],
        capture_output=True,
        text=True
    )
    
    print(f"Extract stdout: {result.stdout}")
    print(f"Extract stderr: {result.stderr}")
    
    return {'extracted': True, 'timestamp': datetime.now().isoformat()}

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
# Предполагаем что есть колонки: price, cost
df_transformed = df.withColumn(
    "margin_pct",
    sql_round(((col("price") - col("cost")) / col("price")) * 100, 2)
)

# Сохранение результата
df_transformed.write.mode("overwrite").parquet("/workspace/staging/products_enriched.parquet")
print(f"Transformed {df_transformed.count()} rows with margin_pct")
spark.stop()
'''
    
    with open('/tmp/transform_products.py', 'w') as f:
        f.write(spark_code)
    
    result = subprocess.run(
        ['spark-submit', '--master', 'local', '/tmp/transform_products.py'],
        capture_output=True,
        text=True
    )
    
    print(f"Transform stdout: {result.stdout}")
    print(f"Transform stderr: {result.stderr}")
    
    return {'transformed': True, 'timestamp': datetime.now().isoformat()}

def load_products(**context):
    """Загрузка результата в Postgres analytics.products_enriched"""
    print("Starting load task...")
    
    from sqlalchemy import create_engine
    import pandas as pd
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("retail_products_load").getOrCreate()
    
    # Чтение трансформированных данных
    df = spark.read.parquet("/workspace/staging/products_enriched.parquet")
    
    # Конвертация в pandas для загрузки в Postgres
    pdf = df.toPandas()
    
    # Подключение к Postgres
    engine = create_engine('postgresql://demo:demo@demo-postgres:5432/analytics')
    
    # Загрузка в таблицу analytics.products_enriched
    pdf.to_sql(
        'products_enriched',
        engine,
        schema='analytics',
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded {len(pdf)} rows to analytics.products_enriched")
    spark.stop()
    
    return {'loaded': True, 'timestamp': datetime.now().isoformat()}

dag = DAG(
    'retail_products_etl',
    default_args=default_args,
    description='ETL пайплайн для продуктов: extract из MySQL, transform с margin_pct, load в Postgres',
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

# Зависимости задач
start >> extract >> transform >> load >> end
