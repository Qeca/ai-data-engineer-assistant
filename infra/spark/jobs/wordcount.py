#!/usr/bin/env python3
"""
PySpark WordCount Script
Классический пример подсчета слов в тексте.
Готов к запуску через spark-submit.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col


def main():
    # Создание SparkSession
    spark = SparkSession.builder \
        .appName("WordCount") \
        .getOrCreate()
    
    # Установка уровня логирования
    spark.sparkContext.setLogLevel("WARN")
    
    # Пример данных для демонстрации
    # В реальном сценарии можно читать из файла: spark.read.text("input.txt")
    data = [
        ("Hello world hello",),
        ("Spark is fast and Spark is powerful",),
        ("PySpark makes big data processing easy",),
        ("Hello Spark world",)
    ]
    
    # Создание DataFrame из примера данных
    df = spark.createDataFrame(data, ["text"])
    
    # Разбиение текста на слова и подсчет
    words_df = df.select(
        explode(split(col("text"), " ")).alias("word")
    )
    
    # Группировка и подсчет
    word_counts = words_df.groupBy("word").count().orderBy(col("count").desc())
    
    # Вывод результатов
    print("=" * 50)
    print("WordCount Results:")
    print("=" * 50)
    word_counts.show(truncate=False)
    
    # Сохранение результатов (опционально)
    # word_counts.write.mode("overwrite").csv("output/wordcount")
    
    # Остановка SparkSession
    spark.stop()
    
    print("=" * 50)
    print("WordCount completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    main()
