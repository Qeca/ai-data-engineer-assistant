from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, current_timestamp
from datetime import datetime

spark = SparkSession.builder.appName("DQ_Orders_CustomerID_Null").getOrCreate()

# Читаем таблицу sales.orders
orders_df = spark.table("sales.orders")

# Вычисляем процент строк с customer_id IS NULL
total_rows = orders_df.count()
null_customer_rows = orders_df.filter(col("customer_id").isNull()).count()

if total_rows > 0:
    null_percentage = (null_customer_rows / total_rows) * 100.0
else:
    null_percentage = 0.0

print(f"Total rows: {total_rows}")
print(f"NULL customer_id rows: {null_customer_rows}")
print(f"NULL percentage: {null_percentage}")

# Создаём DataFrame с результатом
metric_name = "orders_customer_id_null_pct"
value = null_percentage
ts = datetime.now()

result_df = spark.createDataFrame(
    [(metric_name, value, ts)],
    ["metric_name", "value", "ts"]
)

# Создаём схему quality и таблицу dq_metrics через Spark SQL
spark.sql("CREATE SCHEMA IF NOT EXISTS quality")

# Записываем результат
result_df.write.mode("append").saveAsTable("quality.dq_metrics")

print("Metric written to quality.dq_metrics")

# Проверяем запись
spark.sql("SELECT * FROM quality.dq_metrics").show(truncate=False)

spark.stop()
