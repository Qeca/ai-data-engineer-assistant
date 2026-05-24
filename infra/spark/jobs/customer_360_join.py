from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Создаем Spark сессию с JDBC драйверами
spark = SparkSession.builder \
    .appName("Customer360Join") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,mysql:mysql-connector-java:8.0.33,com.clickhouse:clickhouse-jdbc:0.6.0") \
    .getOrCreate()

# Конфигурация подключений
postgres_config = {
    "url": "jdbc:postgresql://demo-postgres:5432/analytics",
    "user": "demo",
    "password": "demo",
    "driver": "org.postgresql.Driver"
}

mysql_config = {
    "url": "jdbc:mysql://demo-mysql:3306/retail_db",
    "user": "demo",
    "password": "demo",
    "driver": "com.mysql.cj.jdbc.Driver"
}

clickhouse_config = {
    "url": "jdbc:clickhouse://demo-clickhouse:8123/analytics",
    "user": "demo",
    "password": "demo",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

# Читаем данные из PostgreSQL (customers)
print("Чтение customers из PostgreSQL...")
customers_df = spark.read \
    .format("jdbc") \
    .option("url", postgres_config["url"]) \
    .option("dbtable", "customers") \
    .option("user", postgres_config["user"]) \
    .option("password", postgres_config["password"]) \
    .option("driver", postgres_config["driver"]) \
    .load()

print(f"Customers загружено: {customers_df.count()} строк")
customers_df.show(5)

# Читаем данные из MySQL (orders)
print("Чтение orders из MySQL...")
orders_df = spark.read \
    .format("jdbc") \
    .option("url", mysql_config["url"]) \
    .option("dbtable", "orders") \
    .option("user", mysql_config["user"]) \
    .option("password", mysql_config["password"]) \
    .option("driver", mysql_config["driver"]) \
    .load()

print(f"Orders загружено: {orders_df.count()} строк")
orders_df.show(5)

# Читаем данные из ClickHouse (events)
print("Чтение events из ClickHouse...")
events_df = spark.read \
    .format("jdbc") \
    .option("url", clickhouse_config["url"]) \
    .option("dbtable", "events") \
    .option("user", clickhouse_config["user"]) \
    .option("password", clickhouse_config["password"]) \
    .option("driver", clickhouse_config["driver"]) \
    .load()

print(f"Events загружено: {events_df.count()} строк")
events_df.show(5)

# Джойним данные по customer_id
print("Выполнение джойна...")
customer_360_df = customers_df \
    .join(orders_df, on="customer_id", how="left") \
    .join(events_df, on="customer_id", how="left")

print(f"Результат джойна: {customer_360_df.count()} строк")
customer_360_df.show(5)

# Сохраняем в Parquet
output_path = "analytics/customer_360.parquet"
print(f"Сохранение в {output_path}...")
customer_360_df.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"Данные успешно сохранены в {output_path}")

# Проверка результата
result_df = spark.read.parquet(output_path)
print(f"Проверка: прочитано {result_df.count()} строк из parquet")
result_df.show(5)

spark.stop()
print("Spark-скрипт выполнен успешно!")
