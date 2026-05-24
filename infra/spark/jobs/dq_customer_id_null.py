from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, current_timestamp
from datetime import datetime

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("DQ_CustomerID_Null_Metric") \
    .getOrCreate()

# Чтение данных из sales.orders
orders_df = spark.read.table("sales.orders")

# Вычисление метрик
total_rows = orders_df.count()
null_customer_rows = orders_df.filter(col("customer_id").isNull()).count()

# Процент NULL значений
null_percentage = (null_customer_rows / total_rows * 100) if total_rows > 0 else 0.0

print(f"Total rows: {total_rows}")
print(f"NULL customer_id rows: {null_customer_rows}")
print(f"NULL percentage: {null_percentage}")

# Подготовка результата для записи
result_df = spark.createDataFrame([
    ("customer_id_null_pct", null_percentage, datetime.now())
], ["metric_name", "value", "ts"])

# Создание таблицы dq_metrics если не существует
spark.sql("""
    CREATE TABLE IF NOT EXISTS dq_metrics (
        metric_name STRING,
        value DOUBLE,
        ts TIMESTAMP
    )
""")

# Запись результата в dq_metrics
result_df.write.mode("append").saveAsTable("dq_metrics")

print("Metric written to dq_metrics successfully")

# Проверка результата
spark.sql("SELECT * FROM dq_metrics ORDER BY ts DESC LIMIT 10").show()

spark.stop()
