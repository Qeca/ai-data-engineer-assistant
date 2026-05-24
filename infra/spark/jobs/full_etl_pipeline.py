"""
Full ETL Pipeline - Spark Job
Извлечение, трансформация и загрузка витрин данных
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, min, max, to_date,
    concat_ws, countDistinct, collect_list
)
from datetime import datetime
import json

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("FullETLPipeline") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("FULL ETL PIPELINE - START")
print(f"Timestamp: {datetime.now().isoformat()}")
print("=" * 60)

# ============================================================================
# STEP 1: EXTRACT - Извлечение данных из источников
# ============================================================================
print("\n[STEP 1] EXTRACT - Извлечение данных...")

# Чтение таблиц из PostgreSQL (через JDBC или как CSV для демо)
# Для демо используем createDataFrame с тестовыми данными
# В продакшене: spark.read.jdbc(...)

# Customers
customers_data = [
    (1, "user1@example.com", "b2b", "2026-05-12 12:00:00"),
    (2, "user2@example.com", "vip", "2026-05-11 12:00:00"),
    (3, "user3@example.com", "retail", "2026-05-10 12:00:00"),
    (4, "user4@example.com", "b2b", "2026-05-09 12:00:00"),
    (5, "user5@example.com", "vip", "2026-05-08 12:00:00"),
]
customers_df = spark.createDataFrame(
    customers_data,
    ["id", "email", "segment", "created_at"]
)
print(f"  ✓ Customers: {customers_df.count()} rows")

# Events
events_data = [
    (1, "2026-05-13 11:53:00", 6, "checkout", "{}"),
    (2, "2026-05-13 11:46:00", 36, "search", "{}"),
    (3, "2026-05-13 11:39:00", 57, "add_to_cart", "{}"),
    (4, "2026-05-13 11:32:00", 165, "page_view", "{}"),
    (5, "2026-05-13 11:25:00", 18, "checkout", "{}"),
    (6, "2026-05-13 11:18:00", 42, "search", "{}"),
    (7, "2026-05-13 11:11:00", 89, "add_to_cart", "{}"),
    (8, "2026-05-13 11:04:00", 123, "page_view", "{}"),
    (9, "2026-05-13 10:57:00", 15, "checkout", "{}"),
    (10, "2026-05-13 10:50:00", 78, "search", "{}"),
]
events_df = spark.createDataFrame(
    events_data,
    ["id", "created_at", "user_id", "event_name", "payload"]
)
print(f"  ✓ Events: {events_df.count()} rows")

# Orders
orders_data = [
    (1, "2026-05-13 12:00:00", 29, 19.05, "paid"),
    (2, "2026-05-13 12:00:00", 58, 69.68, "paid"),
    (3, "2026-05-13 12:00:00", 140, 46.43, "paid"),
    (4, "2026-05-13 12:00:00", 8, 49.41, "paid"),
    (5, "2026-05-13 12:00:00", 155, 19.73, "paid"),
    (6, "2026-05-13 12:00:00", 22, 125.50, "cancelled"),
    (7, "2026-05-13 12:00:00", 33, 89.99, "pending"),
    (8, "2026-05-13 12:00:00", 44, 210.00, "paid"),
    (9, "2026-05-13 12:00:00", 55, 45.00, "cancelled"),
    (10, "2026-05-13 12:00:00", 66, 178.25, "paid"),
]
orders_df = spark.createDataFrame(
    orders_data,
    ["id", "created_at", "user_id", "total_amount", "status"]
)
print(f"  ✓ Orders: {orders_df.count()} rows")

# ============================================================================
# STEP 2: TRANSFORM - Трансформация данных
# ============================================================================
print("\n[STEP 2] TRANSFORM - Трансформация данных...")

# 2.1 Агрегация по сегментам клиентов
print("  → Создаём витрину: customer_segments_agg")
customer_segments_agg = customers_df.groupBy("segment").agg(
    count("id").alias("total_customers"),
    avg(col("id")).alias("avg_customer_id")
).orderBy("segment")
customer_segments_agg.show()

# 2.2 Ежедневная статистика заказов
print("  → Создаём витрину: daily_orders_stats")
daily_orders_stats = orders_df \
    .withColumn("order_date", to_date(col("created_at"))) \
    .groupBy("order_date", "status") \
    .agg(
        count("id").alias("order_count"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value"),
        min("total_amount").alias("min_order_value"),
        max("total_amount").alias("max_order_value")
    ) \
    .orderBy("order_date", "status")
daily_orders_stats.show()

# 2.3 Статистика событий по типам
print("  → Создаём витрину: events_by_type")
events_by_type = events_df.groupBy("event_name").agg(
    count("id").alias("event_count"),
    countDistinct("user_id").alias("unique_users")
).orderBy(col("event_count").desc())
events_by_type.show()

# 2.4 Поведение пользователей (события + заказы)
print("  → Создаём витрину: user_behavior_summary")
user_events = events_df.groupBy("user_id").agg(
    count("id").alias("total_events"),
    countDistinct("event_name").alias("event_types_count"),
    concat_ws(", ", collect_list(col("event_name"))).alias("events_list")
)

user_orders = orders_df.groupBy("user_id").agg(
    count("id").alias("total_orders"),
    sum("total_amount").alias("total_spent"),
    avg("total_amount").alias("avg_order_value"),
    max("total_amount").alias("max_order_value")
)

user_behavior = user_events.join(user_orders, "user_id", "full_outer") \
    .fillna(0, ["total_orders", "total_spent", "avg_order_value", "max_order_value"]) \
    .fillna(0, ["total_events", "event_types_count"]) \
    .fillna("none", ["events_list"]) \
    .orderBy(col("total_spent").desc())
user_behavior.show(truncate=False)

# 2.5 KPI метрики
print("  → Создаём витрину: kpi_metrics")
total_orders = orders_df.count()
total_revenue = orders_df.agg(sum("total_amount")).collect()[0][0]
avg_order_value = orders_df.agg(avg("total_amount")).collect()[0][0]
paid_orders = orders_df.filter(col("status") == "paid").count()
conversion_rate = (paid_orders / total_orders * 100) if total_orders > 0 else 0

kpi_metrics_data = [
    ("total_orders", float(total_orders)),
    ("total_revenue", float(total_revenue) if total_revenue else 0.0),
    ("avg_order_value", float(avg_order_value) if avg_order_value else 0.0),
    ("paid_orders_count", float(paid_orders)),
    ("conversion_rate_pct", conversion_rate),
    ("total_customers", float(customers_df.count())),
    ("total_events", float(events_df.count())),
]
kpi_metrics = spark.createDataFrame(
    kpi_metrics_data,
    ["metric_name", "metric_value"]
)
kpi_metrics.show(truncate=False)

# ============================================================================
# STEP 3: LOAD - Загрузка результатов (в демо - вывод в консоль)
# ============================================================================
print("\n[STEP 3] LOAD - Результаты готовы к загрузке...")

# В продакшене здесь была бы запись в:
# - PostgreSQL: df.write.jdbc(...)
# - Data Lake: df.write.parquet("s3://...")
# - ClickHouse: df.write.format("clickhouse").save(...)

print("  ✓ customer_segments_agg - готово к загрузке")
print("  ✓ daily_orders_stats - готово к загрузке")
print("  ✓ events_by_type - готово к загрузке")
print("  ✓ user_behavior_summary - готово к загрузке")
print("  ✓ kpi_metrics - готово к загрузке")

# ============================================================================
# ИТОГИ
# ============================================================================
print("\n" + "=" * 60)
print("FULL ETL PIPELINE - COMPLETE")
print(f"Timestamp: {datetime.now().isoformat()}")
print("=" * 60)

# Метрики для логирования
result_metrics = {
    "customers_processed": customers_df.count(),
    "events_processed": events_df.count(),
    "orders_processed": orders_df.count(),
    "vitriens_created": 5,
    "vitriens": [
        "customer_segments_agg",
        "daily_orders_stats",
        "events_by_type",
        "user_behavior_summary",
        "kpi_metrics"
    ]
}

print(f"\nРезультаты: {json.dumps(result_metrics, indent=2, ensure_ascii=False)}")

spark.stop()
print("\nSpark session stopped.")
