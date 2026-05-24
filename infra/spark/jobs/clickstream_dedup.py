from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("Clickstream Deduplication") \
    .getOrCreate()

# Чтение clickstream данных
# Предполагаем, что данные содержат: user_id, event_ts, event_type, url, и другие поля
clickstream_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/data/clickstream/raw")

print(f"Исходное количество записей: {clickstream_df.count()}")
print(f"Количество уникальных user_id: {clickstream_df.select('user_id').distinct().count()}")

# Окно для ранжирования записей по user_id и timestamp
window_spec = Window.partitionBy("user_id").orderBy(desc("event_ts"))

# Добавляем ранг и оставляем только последнюю запись для каждого user_id
deduped_df = clickstream_df \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

print(f"Количество записей после дедупликации: {deduped_df.count()}")

# Сохранение результата
deduped_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/data/clickstream/deduped")

print("Дедупликация завершена успешно!")

spark.stop()
