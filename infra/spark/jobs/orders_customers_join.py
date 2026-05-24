from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("OrdersCustomersJoin") \
    .getOrCreate()

# Чтение таблиц из Spark catalog
customers = spark.table("sales.customers")
orders = spark.table("sales.orders")

# Inner Join: заказы с информацией о клиентах
orders_with_customers = orders.join(
    customers,
    on="customer_id",
    how="inner"
)

# Left Join: все заказы + клиенты (если есть)
orders_left_customers = orders.join(
    customers,
    on="customer_id",
    how="left"
)

# Пример агрегации: сумма заказов по сегментам клиентов
orders_by_segment = orders.join(
    customers,
    on="customer_id",
    how="inner"
).groupBy("segment").agg(
    count("order_id").alias("order_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount")
)

# Вывод результатов
print("=== Orders with Customers (Inner Join) ===")
orders_with_customers.show(10, truncate=False)

print("=== Orders by Customer Segment ===")
orders_by_segment.show(truncate=False)

# Сохранение результата (опционально)
# orders_with_customers.write.mode("overwrite").saveAsTable("sales.orders_with_customers")

spark.stop()
