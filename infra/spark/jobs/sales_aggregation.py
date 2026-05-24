from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, round as _round

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("SalesAggregation") \
    .getOrCreate()

# Чтение данных из таблиц (подключение настраивается через Spark config)
customers_df = spark.read.table("sales.customers")
orders_df = spark.read.table("sales.orders")

# Join таблиц по customer_id
joined_df = orders_df.join(customers_df, on="customer_id", how="inner")

# Агрегация по сегменту клиента
aggregated_df = joined_df.groupBy("segment").agg(
    _sum("amount").alias("total_amount"),
    count("order_id").alias("order_count"),
    _round(avg("amount"), 2).alias("avg_order_amount"),
    count(col("order_id").filter(col("status") == "paid")).alias("paid_orders")
).orderBy(col("total_amount").desc())

# Вывод результатов
print("=== Агрегация продаж по сегментам клиентов ===")
aggregated_df.show(truncate=False)

# Сохранение результатов в таблицу
aggregated_df.write.mode("overwrite").saveAsTable("sales.aggregation_results")

spark.stop()
print("Spark job completed successfully!")
