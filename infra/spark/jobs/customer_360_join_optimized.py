from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

# Создаем Spark сессию с увеличенной памятью и оптимизациями
spark = SparkSession.builder \
    .appName("Customer360JoinOptimized") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,mysql:mysql-connector-java:8.0.33,com.clickhouse:clickhouse-jdbc:0.6.0") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Читаем данные из PostgreSQL (customers) - обычно это маленькая таблица
print("Чтение customers из PostgreSQL...")
customers_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://demo-postgres:5432/analytics") \
    .option("dbtable", "customers") \
    .option("user", "demo") \
    .option("driver", "org.postgresql.Driver") \
    .option("fetchsize", "10000") \
    .option("numPartitions", "4") \
    .option("partitionColumn", "customer_id") \
    .option("lowerBound", "1") \
    .option("upperBound", "100000") \
    .load()

print(f"Customers загружено: {customers_df.count()} строк")

# Читаем данные из MySQL (orders) - большая таблица, разбиваем на partition'ы
print("Чтение orders из MySQL...")
orders_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://demo-mysql:3306/retail_db") \
    .option("dbtable", "orders") \
    .option("user", "demo") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("fetchsize", "10000") \
    .option("numPartitions", "8") \
    .option("partitionColumn", "order_id") \
    .option("lowerBound", "1") \
    .option("upperBound", "1000000") \
    .load()

print(f"Orders загружено: {orders_df.count()} строк")

# Читаем данные из ClickHouse (events) - большая таблица
print("Чтение events из ClickHouse...")
events_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://demo-clickhouse:8123/analytics") \
    .option("dbtable", "events") \
    .option("user", "demo") \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("fetchsize", "10000") \
    .option("numPartitions", "8") \
    .load()

print(f"Events загружено: {events_df.count()} строк")

# Оптимизированный джойн:
# 1. customers - маленькая таблица, используем broadcast
# 2. orders и events - большие, используем sort-merge join
print("Выполнение оптимизированного джойна...")

# Сначала джойним orders с customers (broadcast для customers)
orders_with_customers = orders_df.join(
    broadcast(customers_df),
    on="customer_id",
    how="left"
)

# Затем джойним с events (repartition для балансировки)
customer_360_df = orders_with_customers \
    .repartition(16, col("customer_id")) \
    .join(
        events_df.repartition(16, col("customer_id")),
        on="customer_id",
        how="left"
    )

print(f"Результат джойна: {customer_360_df.count()} строк")
customer_360_df.show(5)

# Сохраняем в Parquet с compression
output_path = "analytics/customer_360_optimized.parquet"
print(f"Сохранение в {output_path}...")
customer_360_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

print(f"Данные успешно сохранены в {output_path}")

spark.stop()
print("Spark-скрипт выполнен успешно!")
