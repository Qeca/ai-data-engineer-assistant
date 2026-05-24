from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, desc

# Инициализация Spark сессии
spark = SparkSession.builder \
    .appName("Clickstream Deduplication by User") \
    .getOrCreate()

# Конфигурация
INPUT_TABLE = "analytics.clickstream_raw"
OUTPUT_TABLE = "analytics.clickstream_dedup"
PARTITION_COLUMN = "event_date"

def deduplicate_clickstream():
    """
    Дедупликация clickstream данных по user_id.
    Для каждого user_id оставляем последнюю запись по event_ts.
    """
    print(f"Чтение данных из {INPUT_TABLE}...")
    
    # Чтение исходных данных
    df = spark.table(INPUT_TABLE)
    
    print(f"Всего записей до дедупликации: {df.count()}")
    print(f"Уникальных user_id: {df.select('user_id').distinct().count()}")
    
    # Определение окна для ранжирования записей по user_id
    # Сортируем по event_ts descending, чтобы последняя запись получила rank 1
    window_spec = Window.partitionBy("user_id").orderBy(desc("event_ts"))
    
    # Добавляем ранг и фильтруем только первые записи
    df_dedup = df.withColumn("rn", row_number().over(window_spec)) \
                 .filter(col("rn") == 1) \
                 .drop("rn")
    
    print(f"Записей после дедупликации: {df_dedup.count()}")
    
    # Сохранение результата
    # Используем overwrite mode для полной перезаписи или merge для инкрементальной
    df_dedup.write.mode("overwrite").saveAsTable(OUTPUT_TABLE)
    
    print(f"Результат сохранён в {OUTPUT_TABLE}")
    
    # Вывод статистики
    df_dedup.show(10, truncate=False)
    
    return df_dedup

if __name__ == "__main__":
    try:
        result = deduplicate_clickstream()
        print("Дедупликация завершена успешно!")
    except Exception as e:
        print(f"Ошибка при выполнении: {e}")
        raise
    finally:
        spark.stop()
