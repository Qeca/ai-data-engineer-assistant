from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, datediff, current_date, max as max_, when

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("RFM_Features") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Параметры подключения к Postgres (demo-postgres-warehouse)
jdbc_url = "jdbc:postgresql://demo-postgres:5432/analytics"
jdbc_properties = {
    "user": "demo",
    "password": "demo_pass",
    "driver": "org.postgresql.Driver"
}

# Чтение данных из sales.orders
orders_df = spark.read.jdbc(
    url=jdbc_url,
    table="sales.orders",
    properties=jdbc_properties
)

# Фильтрация только оплаченных заказов
orders_df = orders_df.filter(col("status") == "paid")

# Расчет даты 90 дней назад
current_date_col = current_date()
date_90_days_ago = current_date_col - 90

# RFM агрегация по customer_id
rfm_df = orders_df.groupBy("customer_id").agg(
    # Recency: дни с последнего заказа
    datediff(current_date_col, max_(col("order_ts").cast("date"))).alias("recency"),
    
    # Frequency: число заказов за последние 90 дней
    count(
        when(col("order_ts").cast("date") >= date_90_days_ago, 1)
    ).alias("frequency"),
    
    # Monetary: сумма заказов за последние 90 дней
    sum(
        when(col("order_ts").cast("date") >= date_90_days_ago, col("amount"))
    ).alias("monetary")
).withColumn(
    "monetary", 
    col("monetary").cast("double")
)

# Сохранение результата в Parquet
rfm_df.write.mode("overwrite").parquet("features/rfm.parquet")

print(f"RFM features saved. Total customers: {rfm_df.count()}")
rfm_df.show(truncate=False)

spark.stop()
