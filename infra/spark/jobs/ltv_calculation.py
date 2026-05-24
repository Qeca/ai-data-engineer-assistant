from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, datediff, current_date, max as spark_max, min as spark_min, to_date

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("LTV_Calculation") \
    .getOrCreate()

# Чтение данных о клиентах и заказах из схемы sales
customers_df = spark.read.table("sales.customers")
orders_df = spark.read.table("sales.orders")

# Фильтрация только оплаченных заказов для расчёта LTV
paid_orders_df = orders_df.filter(col("status") == "paid")

# Расчёт LTV для каждого клиента
# LTV = сумма всех оплаченных заказов клиента
ltv_df = paid_orders_df.groupBy("customer_id").agg(
    spark_sum("amount").alias("total_revenue"),
    count("order_id").alias("order_count"),
    avg("amount").alias("avg_order_value"),
    spark_max("order_ts").alias("last_order_date"),
    spark_min("order_ts").alias("first_order_date")
)

# Добавляем информацию о клиентах
ltv_with_customers = ltv_df.join(
    customers_df,
    ltv_df.customer_id == customers_df.customer_id,
    "left"
).select(
    ltv_df.customer_id,
    col("customers.email").alias("customer_email"),
    col("customers.segment").alias("customer_segment"),
    ltv_df.total_revenue,
    ltv_df.order_count,
    ltv_df.avg_order_value,
    ltv_df.first_order_date,
    ltv_df.last_order_date,
    datediff(current_date(), to_date(ltv_df.first_order_date)).alias("customer_lifetime_days")
)

# Расчёт LTV с учётом времени жизни клиента (LTV в день)
ltv_final = ltv_with_customers.withColumn(
    "ltv_per_day",
    col("total_revenue") / (col("customer_lifetime_days") + 1)
)

# Вывод результатов
print("=== LTV Calculation Results ===")
ltv_final.orderBy(col("total_revenue").desc()).show(20, truncate=False)

# Сохранение результатов в таблицу
ltv_final.write.mode("overwrite").saveAsTable("sales.customer_ltv")

print("LTV calculation completed. Results saved to 'sales.customer_ltv' table.")

spark.stop()
