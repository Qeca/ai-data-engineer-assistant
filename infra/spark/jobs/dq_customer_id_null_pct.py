from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, current_timestamp
from datetime import datetime

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("DQ Customer ID Null Percentage") \
    .getOrCreate()

# Чтение данных из sales.orders через Spark catalog
orders_df = spark.table("sales.orders")

# Вычисление процента строк с customer_id IS NULL
total_count = orders_df.count()
null_count = orders_df.filter(col("customer_id").isNull()).count()

if total_count > 0:
    null_pct = (null_count / total_count) * 100.0
else:
    null_pct = 0.0

print(f"Total rows: {total_count}")
print(f"NULL customer_id rows: {null_count}")
print(f"NULL percentage: {null_pct}")

# Подготовка результата для записи
result_df = spark.createDataFrame([
    ("customer_id_null_pct", null_pct, datetime.now())
], ["metric_name", "value", "ts"])

# Создание схемы quality если не существует
spark.sql("CREATE SCHEMA IF NOT EXISTS quality")

# Создание таблицы quality.dq_metrics если не существует
spark.sql("""
    CREATE TABLE IF NOT EXISTS quality.dq_metrics (
        metric_name STRING,
        value DOUBLE,
        ts TIMESTAMP
    )
""")

# Вставка результата через insertInto (использует Hive catalog)
result_df.write.insertInto("quality.dq_metrics")

print("Result written to quality.dq_metrics")

# Проверка результата
print("Current dq_metrics:")
spark.sql("SELECT * FROM quality.dq_metrics ORDER BY ts DESC LIMIT 10").show(truncate=False)

spark.stop()