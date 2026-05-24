from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, to_date, coalesce, lit

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("DailyRevenueAggregation") \
    .getOrCreate()

# Читаем данные из parquet файлов
# Путь: infra/spark/data/sales/*.parquet
df = spark.read.parquet("infra/spark/data/sales/*.parquet")

# Показываем схему для отладки
print("Input schema:")
df.printSchema()
print(f"Input row count: {df.count()}")

# Агрегируем выручку по дням
# Ожидаемые колонки: order_ts/sale_date (дата), amount/revenue (сумма)
# Адаптируем под фактическую схему данных
daily_revenue = df.groupBy(
    to_date(col("order_ts")).alias("date")
).agg(
    coalesce(spark_sum(col("amount")), lit(0)).alias("total_revenue")
).orderBy("date")

# Показываем результат
print("Daily revenue aggregation:")
daily_revenue.show(truncate=False)

# Пишем результат в parquet
# Путь: analytics/daily_revenue.parquet
daily_revenue.write.mode("overwrite").parquet("analytics/daily_revenue.parquet")

print("Daily revenue written to analytics/daily_revenue.parquet")

spark.stop()