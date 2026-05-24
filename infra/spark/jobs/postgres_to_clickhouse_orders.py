from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp

# Инициализация Spark сессии с JDBC драйверами
spark = SparkSession.builder \
    .appName("PostgresToClickHouseOrders") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.6.0") \
    .getOrCreate()

# Параметры подключения
postgres_host = "demo-postgres"
postgres_port = 5432
postgres_db = "analytics"
postgres_user = "demo"
postgres_password = "demo"

clickhouse_host = "demo-clickhouse"
clickhouse_port = 8123
clickhouse_db = "analytics"
clickhouse_user = "demo"
clickhouse_password = "demo"

# Чтение данных из Postgres
jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
orders_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "sales.orders") \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"Прочитано строк из Postgres: {orders_df.count()}")
orders_df.show(5)

# Трансформация: status='paid' → event_type='purchase'
events_df = orders_df.select(
    col("order_id").alias("event_id"),
    col("customer_id"),
    col("order_ts").alias("event_ts"),
    col("amount"),
    when(col("status") == "paid", lit("purchase"))
        .otherwise(col("status"))
        .alias("event_type"),
    current_timestamp().alias("processed_at")
)

print("После трансформации:")
events_df.show(5)

# Запись в ClickHouse
clickhouse_url = f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}/{clickhouse_db}"
events_df.write \
    .format("jdbc") \
    .option("url", clickhouse_url) \
    .option("dbtable", "analytics.events") \
    .option("user", clickhouse_user) \
    .option("password", clickhouse_password) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .mode("append") \
    .save()

print("Данные успешно загружены в ClickHouse analytics.events")

spark.stop()
