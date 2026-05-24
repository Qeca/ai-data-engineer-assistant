from pyspark.sql import SparkSession

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("CountParquetRows") \
    .getOrCreate()

# Путь к файлам
data_path = "infra/spark/data/sales/*.parquet"

# Читаем все parquet файлы
df = spark.read.parquet(data_path)

# Считаем общее количество строк
total_rows = df.count()

# Для подсчета строк по каждому файлу используем input_file_name
from pyspark.sql.functions import input_file_name, count as spark_count

df_with_filename = df.withColumn("filename", input_file_name())

# Группируем по имени файла и считаем строки
rows_per_file = df_with_filename.groupBy("filename").agg(
    spark_count("*").alias("row_count")
)

# Выводим результат
print("=" * 60)
print("Количество строк в каждом файле:")
print("=" * 60)
rows_per_file.show(truncate=False)

print("=" * 60)
print(f"Общее количество строк: {total_rows}")
print("=" * 60)

spark.stop()