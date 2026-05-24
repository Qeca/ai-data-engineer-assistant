from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, 
    min as spark_min, datediff, current_date, to_date, 
    round as spark_round, when, lit, rank, window
)
from pyspark.sql.window import Window

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("LTV_Calculation_v3") \
    .getOrCreate()

print("=== Starting LTV Calculation v3 ===")

# Чтение данных о клиентах и заказах из схемы sales
customers_df = spark.read.table("sales.customers")
orders_df = spark.read.table("sales.orders")

print(f"Customers loaded: {customers_df.count()} rows")
print(f"Orders loaded: {orders_df.count()} rows")

# Фильтрация только оплаченных заказов для расчёта LTV
paid_orders_df = orders_df.filter(col("status") == "paid")
print(f"Paid orders: {paid_orders_df.count()} rows")

# Базовые метрики по клиентам
customer_metrics = paid_orders_df.groupBy("customer_id").agg(
    spark_sum("amount").alias("total_revenue"),
    count("order_id").alias("order_count"),
    avg("amount").alias("avg_order_value"),
    spark_max("order_ts").alias("last_order_date"),
    spark_min("order_ts").alias("first_order_date"),
    spark_max("order_ts").alias("recency_date")
)

# Добавляем информацию о клиентах
customer_ltv = customer_metrics.join(
    customers_df,
    customer_metrics.customer_id == customers_df.customer_id,
    "left"
).select(
    customer_metrics.customer_id,
    col("customers.email").alias("customer_email"),
    col("customers.segment").alias("customer_segment"),
    col("customers.created_at").alias("customer_created_at"),
    customer_metrics.total_revenue,
    customer_metrics.order_count,
    customer_metrics.avg_order_value,
    customer_metrics.first_order_date,
    customer_metrics.last_order_date,
    datediff(current_date(), to_date(customer_metrics.first_order_date)).alias("customer_lifetime_days"),
    datediff(current_date(), to_date(customer_metrics.recency_date)).alias("days_since_last_order")
)

# Расчёт LTV в день
customer_ltv = customer_ltv.withColumn(
    "ltv_per_day",
    spark_round(col("total_revenue") / (col("customer_lifetime_days") + 1), 2)
)

# RFM анализ
# Recency Score (чем меньше дней с последнего заказа, тем выше score)
customer_ltv = customer_ltv.withColumn(
    "recency_score",
    when(col("days_since_last_order") <= 7, 5)
    .when(col("days_since_last_order") <= 30, 4)
    .when(col("days_since_last_order") <= 90, 3)
    .when(col("days_since_last_order") <= 180, 2)
    .otherwise(1)
)

# Frequency Score (чем больше заказов, тем выше score)
customer_ltv = customer_ltv.withColumn(
    "frequency_score",
    when(col("order_count") >= 10, 5)
    .when(col("order_count") >= 5, 4)
    .when(col("order_count") >= 3, 3)
    .when(col("order_count") >= 2, 2)
    .otherwise(1)
)

# Monetary Score (чем выше выручка, тем выше score)
window_spec = Window.orderBy(col("total_revenue").desc())
customer_ltv = customer_ltv.withColumn(
    "revenue_percentile",
    rank().over(window_spec)
)

total_customers = customer_ltv.count()
customer_ltv = customer_ltv.withColumn(
    "monetary_score",
    when(col("revenue_percentile") <= total_customers * 0.2, 5)
    .when(col("revenue_percentile") <= total_customers * 0.4, 4)
    .when(col("revenue_percentile") <= total_customers * 0.6, 3)
    .when(col("revenue_percentile") <= total_customers * 0.8, 2)
    .otherwise(1)
)

# RFM Score (сумма всех scores)
customer_ltv = customer_ltv.withColumn(
    "rfm_score",
    col("recency_score") + col("frequency_score") + col("monetary_score")
)

# Сегментация клиентов по RFM
customer_ltv = customer_ltv.withColumn(
    "customer_segment_rfm",
    when(col("rfm_score") >= 13, "Champions")
    .when(col("rfm_score") >= 10, "Loyal Customers")
    .when(col("rfm_score") >= 8, "Potential Loyalists")
    .when(col("rfm_score") >= 6, "At Risk")
    .otherwise("Lost")
)

# Прогноз LTV на 12 месяцев (упрощённая модель)
customer_ltv = customer_ltv.withColumn(
    "ltv_12m_forecast",
    spark_round(
        col("ltv_per_day") * 365 * 
        when(col("customer_segment_rfm") == "Champions", 1.2)
        .when(col("customer_segment_rfm") == "Loyal Customers", 1.1)
        .when(col("customer_segment_rfm") == "Potential Loyalists", 1.0)
        .when(col("customer_segment_rfm") == "At Risk", 0.7)
        .otherwise(0.5),
        2
    )
)

# Вывод результатов
print("\n=== LTV Calculation Results ===")
customer_ltv.orderBy(col("total_revenue").desc()).show(20, truncate=False)

print("\n=== RFM Segmentation Summary ===")
customer_ltv.groupBy("customer_segment_rfm").agg(
    count("*").alias("customer_count"),
    spark_round(avg("total_revenue"), 2).alias("avg_revenue"),
    spark_round(avg("ltv_12m_forecast"), 2).alias("avg_ltv_forecast")
).orderBy(col("customer_count").desc()).show(truncate=False)

# Сохранение результатов в таблицу
customer_ltv.drop("revenue_percentile").write.mode("overwrite").saveAsTable("sales.customer_ltv_v3")

print("\nLTV calculation v3 completed. Results saved to 'sales.customer_ltv_v3' table.")

spark.stop()
