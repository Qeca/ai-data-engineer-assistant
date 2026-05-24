from pyspark.sql import SparkSession
from pyspark.sql.functions import col, percentile_approx, min, max, avg

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("Orders Amount Median") \
    .getOrCreate()

# Читаем таблицу orders через JDBC
# Подключение настраивается через Spark конфигурацию
orders_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/sales") \
    .option("dbtable", "sales.orders") \
    .option("user", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"Total orders: {orders_df.count()}")
print(f"Schema:")
orders_df.printSchema()

# Вычисляем медиану по amount используя approxQuantile
# Второй параметр - точность (0.01 = 1% погрешность)
median_result = orders_df.approxQuantile("amount", [0.5], 0.01)
median_value = median_result[0]

print(f"\n=== Median amount: {median_value} ===")

# Альтернативный способ через SQL с percentile_approx
orders_df.createOrReplaceTempView("orders")
stats_df = spark.sql("""
    SELECT 
        percentile_approx(amount, 0.5) as median_amount,
        min(amount) as min_amount,
        max(amount) as max_amount,
        avg(amount) as avg_amount,
        count(*) as total_count
    FROM orders
""")
print("\nStatistics via SQL:")
stats_df.show(truncate=False)

spark.stop()
