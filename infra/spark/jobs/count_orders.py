from pyspark.sql import SparkSession

# Создаём Spark сессию
spark = SparkSession.builder \
    .appName("CountOrders") \
    .getOrCreate()

# Читаем таблицу orders
# Подключение к БД настраивается через Spark конфигурацию (spark-defaults.conf)
# или через предварительно зарегистрированный JDBC connection
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/sales") \
    .option("dbtable", "sales.orders") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Подсчитываем количество строк
row_count = df.count()

print(f"Количество строк в таблице orders: {row_count}")

# Показываем превью данных
df.show(truncate=False)

spark.stop()