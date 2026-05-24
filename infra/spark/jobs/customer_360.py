from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Создаем Spark сессию с JDBC драйверами
spark = SparkSession.builder \
    .appName("Customer360") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,mysql:mysql-connector-java:8.0.33,ru.yandex.clickhouse:clickhouse-jdbc:0.6.0") \
    .getOrCreate()

# JDBC параметры для PostgreSQL (customers)
postgres_url = "jdbc:postgresql://demo-postgres:5432/analytics"
postgres_properties = {
    "user": "demo",
    "password": "demo",
    "driver": "org.postgresql.Driver"
}

# JDBC параметры для MySQL (orders)
mysql_url = "jdbc:mysql://demo-mysql:3306/analytics"
mysql_properties = {
    "user": "demo",
    "password": "demo",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# JDBC параметры для ClickHouse (events)
clickhouse_url = "jdbc:clickhouse://demo-clickhouse:8123/analytics"
clickhouse_properties = {
    "user": "demo",
    "password": "demo",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver"
}

# Читаем данные из PostgreSQL - таблица customers
print("Чтение customers из PostgreSQL...")
customers_df = spark.read.jdbc(
    url=postgres_url,
    table="sales.customers",
    properties=postgres_properties
)
print(f"customers: {customers_df.count()} строк")
customers_df.show(5)

# Читаем данные из MySQL - таблица orders
print("Чтение orders из MySQL...")
orders_df = spark.read.jdbc(
    url=mysql_url,
    table="orders",
    properties=mysql_properties
)
print(f"orders: {orders_df.count()} строк")
orders_df.show(5)

# Читаем данные из ClickHouse - таблица events
print("Чтение events из ClickHouse...")
events_df = spark.read.jdbc(
    url=clickhouse_url,
    table="events",
    properties=clickhouse_properties
)
print(f"events: {events_df.count()} строк")
events_df.show(5)

# Выполняем джойн по customer_id
# customers LEFT JOIN orders LEFT JOIN events
print("Выполнение джойна...")
customer_360_df = customers_df \
    .join(orders_df, on="customer_id", how="left") \
    .join(events_df, on="customer_id", how="left")

print(f"Результат джойна: {customer_360_df.count()} строк")
customer_360_df.show(10, truncate=False)

# Сохраняем в Parquet файл
output_path = "analytics/customer_360.parquet"
print(f"Сохранение в {output_path}...")
customer_360_df.write.mode("overwrite").parquet(output_path)
print(f"Данные успешно сохранены в {output_path}")

# Показываем схему результата
print("Схема результата:")
customer_360_df.printSchema()

spark.stop()
print("Spark сессия завершена")
