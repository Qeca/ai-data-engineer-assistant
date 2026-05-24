from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("Orders Median Amount") \
    .getOrCreate()

# Чтение данных из таблицы orders
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/sales") \
    .option("dbtable", "sales.orders") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Вычисление медианы через approxQuantile (50-й перцентиль)
median_amount = df.approxQuantile("amount", [0.5], 0.01)[0]

print(f"Median amount: {median_amount}")

# Альтернативный способ через SQL
df.createOrReplaceTempView("orders")
median_sql = spark.sql("""
    SELECT percentile_approx(amount, 0.5) as median_amount
    FROM orders
""")
median_sql.show()

spark.stop()