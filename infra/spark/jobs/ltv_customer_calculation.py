from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, datediff, current_date, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("LTV Customer Calculation") \
    .getOrCreate()

# Схема для таблицы customers
customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("email", StringType(), False),
    StructField("segment", StringType(), False),
    StructField("created_at", TimestampType(), False)
])

# Схема для таблицы orders
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_ts", TimestampType(), False),
    StructField("amount", DoubleType(), False),
    StructField("status", StringType(), False)
])

# Чтение данных из PostgreSQL (в реальном сценарии используется JDBC)
# Для демонстрации создаём тестовые данные
customers_data = [
    (1, "anna@example.com", "retail", "2026-05-01 09:00:00"),
    (2, "ivan@example.com", "b2b", "2026-05-02 10:00:00"),
    (3, "maria@example.com", "vip", "2026-05-03 11:00:00")
]

orders_data = [
    (1001, 1, "2026-05-10 10:15:00", 1290.50, "paid"),
    (1002, 2, "2026-05-10 11:40:00", 5600.00, "paid"),
    (1003, 3, "2026-05-11 12:20:00", 990.00, "cancelled"),
    (1004, 1, "2026-05-11 13:05:00", 2400.30, "paid"),
    (1005, 3, "2026-05-12 18:45:00", 10200.00, "paid")
]

# Создание DataFrame
customers_df = spark.createDataFrame(customers_data, schema=customers_schema)
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Фильтрация только оплаченных заказов
paid_orders_df = orders_df.filter(col("status") == "paid")

# Расчёт LTV для каждого клиента
# LTV = сумма всех оплаченных заказов клиента
ltv_df = paid_orders_df.groupBy("customer_id").agg(
    spark_sum("amount").alias("ltv_total"),
    count("order_id").alias("order_count"),
    avg("amount").alias("avg_order_value"),
    spark_min("order_ts").alias("first_order_date"),
    spark_max("order_ts").alias("last_order_date")
)

# Добавление информации о клиентах (сегмент)
ltv_with_customers = ltv_df.join(
    customers_df.select("customer_id", "email", "segment", "created_at"),
    on="customer_id",
    how="left"
)

# Расчёт дополнительных метрик
ltv_final = ltv_with_customers.withColumn(
    "customer_tenure_days",
    datediff(current_date(), col("created_at"))
).withColumn(
    "ltv_per_day",
    col("ltv_total") / (col("customer_tenure_days") + 1)  # +1 чтобы избежать деления на 0
).select(
    col("customer_id"),
    col("email"),
    col("segment"),
    col("ltv_total").round(2).alias("ltv_total"),
    col("order_count"),
    col("avg_order_value").round(2).alias("avg_order_value"),
    col("first_order_date"),
    col("last_order_date"),
    col("customer_tenure_days"),
    col("ltv_per_day").round(2).alias("ltv_per_day")
).orderBy(col("ltv_total").desc())

# Вывод результатов
print("=== LTV Customer Calculation Results ===")
ltv_final.show(truncate=False)

# Сохранение результатов (в реальном сценарии - в БД или файл)
# ltv_final.write.mode("overwrite").jdbc(url, "analytics.customer_ltv", properties)

print(f"Total customers with LTV: {ltv_final.count()}")

spark.stop()