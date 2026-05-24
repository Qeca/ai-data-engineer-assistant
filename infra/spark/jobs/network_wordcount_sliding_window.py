#!/usr/bin/env python3
"""
Spark Structured Streaming: Word Count with Sliding Window

Подсчитывает слова в UTF8 тексте, разделенном '\n', поступающем из сети.
Использует скользящее окно настраиваемой длительности.

Ключевые классы и функции:
- pyspark.sql.SparkSession: основная точка входа
- pyspark.sql.functions.cast: преобразование типов (CAST)
- pyspark.sql.functions.window: скользящие временные окна
- pyspark.sql.functions.split, explode: токенизация текста

Запуск:
    spark-submit network_wordcount_sliding_window.py <hostname> <port> <window_duration> <slide_duration>

Пример:
    spark-submit network_wordcount_sliding_window.py localhost 9999 10 5

Аргументы:
    hostname: хост для подключения (например, localhost)
    port: порт для подключения (например, 9999)
    window_duration: длительность окна в секундах (например, 10)
    slide_duration: шаг скольжения в секундах (например, 5)
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode,
    split,
    window,
    col,
    from_unixtime,
    current_timestamp,
    lit,
    trim,
    lower,
    length
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType


def create_spark_session(app_name="NetworkWordCountSlidingWindow"):
    """
    Создает SparkSession для Structured Streaming.
    
    Returns:
        SparkSession: настроенная сессия Spark
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()
    
    # Уменьшаем уровень логирования для чистоты вывода
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_stream_from_socket(spark, hostname, port):
    """
    Читает поток данных из сетевого сокета.
    
    Args:
        spark: SparkSession
        hostname: хост для подключения
        port: порт для подключения
    
    Returns:
        DataFrame: поток строк из сокета
    """
    lines = spark.readStream \
        .format("socket") \
        .option("host", hostname) \
        .option("port", port) \
        .option("includeTimestamp", "false") \
        .load()
    
    return lines


def parse_lines_with_timestamp(lines):
    """
    Парсит строки формата: timestamp|text
    где timestamp - Unix timestamp в секундах.
    
    Использует CAST для преобразования типов:
    - timestamp_str CAST AS DOUBLE для from_unixtime
    - результат CAST AS TIMESTAMP
    
    Args:
        lines: DataFrame с колонкой 'value'
    
    Returns:
        DataFrame с колонками 'text' и 'event_time'
    """
    # Разделяем строку на timestamp и текст по разделителю '|'
    parsed = lines.select(
        split(col("value"), "\\|", 2).alias("parts")
    ).select(
        col("parts").getItem(0).alias("timestamp_str"),
        col("parts").getItem(1).alias("text")
    )
    
    # CAST: преобразуем строку timestamp в DOUBLE для from_unixtime
    # from_unixtime возвращает TIMESTAMP
    parsed = parsed.withColumn(
        "event_time",
        from_unixtime(col("timestamp_str").cast(DoubleType()))
    ).drop("timestamp_str")
    
    return parsed


def parse_lines_auto_timestamp(lines):
    """
    Альтернативный парсер: если данные без явного timestamp,
    использует время получения строки как event_time.
    
    Args:
        lines: DataFrame с колонкой 'value'
    
    Returns:
        DataFrame с колонками 'text' и 'event_time'
    """
    parsed = lines.select(
        col("value").alias("text"),
        current_timestamp().alias("event_time")
    )
    return parsed


def count_words_in_sliding_window(df, window_duration_seconds, slide_duration_seconds):
    """
    Применяет скользящее окно и подсчитывает слова.
    
    Использует CAST для явного указания типов в window функции.
    
    Args:
        df: DataFrame с колонками 'text' и 'event_time'
        window_duration_seconds: длительность окна в секундах
        slide_duration_seconds: шаг скольжения окна в секундах
    
    Returns:
        DataFrame с колонками: window, word, count
    """
    # Форматируем длительности для Spark window функции
    window_duration = f"{window_duration_seconds} seconds"
    slide_duration = f"{slide_duration_seconds} seconds"
    
    # Разбиваем текст на слова:
    # 1. trim - удаляем пробелы по краям
    # 2. lower - приводим к нижнему регистру для нормализации
    # 3. split - разбиваем по пробельным символам
    # 4. explode - превращаем массив слов в строки
    # 5. length > 0 - фильтруем пустые строки
    words = df.select(
        explode(split(lower(trim(col("text"))), "\\s+")).alias("word"),
        col("event_time")
    ).filter(
        length(col("word")) > 0
    )
    
    # Применяем скользящее окно и группируем
    # window() создает структурную колонку с полями start и end
    windowed_counts = words \
        .groupBy(
            window(col("event_time"), window_duration, slide_duration),
            col("word")
        ) \
        .count() \
        .withColumnRenamed("count", "word_count") \
        .orderBy(
            col("window").asc(),
            col("word_count").desc()
        )
    
    return windowed_counts


def write_output_to_console(df):
    """
    Выводит результаты в консоль в режиме complete.
    
    Args:
        df: DataFrame для вывода
    
    Returns:
        StreamingQuery: активный запрос
    """
    query = df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "20") \
        .start()
    
    return query


def write_output_to_memory(df, spark, query_name="word_counts"):
    """
    Выводит результаты в память для программного доступа.
    Полезно для тестирования и интеграции.
    
    Args:
        df: DataFrame для вывода
        spark: SparkSession
        query_name: имя запроса для регистрации в памяти
    
    Returns:
        StreamingQuery: активный запрос
    """
    query = df.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName(query_name) \
        .start()
    
    return query


def main():
    """
    Основная функция запуска Spark Streaming job.
    
    Ожидает аргументы командной строки:
        <hostname> <port> <window_duration_seconds> <slide_duration_seconds>
    """
    # Парсим аргументы командной строки
    if len(sys.argv) < 5:
        print("Usage: network_wordcount_sliding_window.py <hostname> <port> <window_duration> <slide_duration>")
        print("")
        print("Arguments:")
        print("  hostname: хост для подключения (например, localhost)")
        print("  port: порт для подключения (например, 9999)")
        print("  window_duration: длительность окна в секундах (например, 10)")
        print("  slide_duration: шаг скольжения в секундах (например, 5)")
        print("")
        print("Example:")
        print("  spark-submit network_wordcount_sliding_window.py localhost 9999 10 5")
        print("")
        print("Формат входных данных (каждая строка):")
        print("  <unix_timestamp>|<текст>")
        print("  Пример: 1699900000|hello world from network")
        sys.exit(1)
    
    hostname = sys.argv[1]
    port = int(sys.argv[2])
    window_duration_seconds = int(sys.argv[3])
    slide_duration_seconds = int(sys.argv[4])
    
    print("=" * 60)
    print("Network Word Count with Sliding Window")
    print("=" * 60)
    print(f"Host: {hostname}")
    print(f"Port: {port}")
    print(f"Window Duration: {window_duration_seconds} seconds")
    print(f"Slide Duration: {slide_duration_seconds} seconds")
    print("=" * 60)
    
    # Создаем SparkSession
    spark = create_spark_session()
    
    try:
        # Читаем поток из сокета
        print("Connecting to socket stream...")
        lines = read_stream_from_socket(spark, hostname, port)
        
        # Парсим строки с временными метками (формат: timestamp|text)
        print("Parsing lines with timestamp...")
        parsed = parse_lines_with_timestamp(lines)
        
        # Подсчитываем слова в скользящих окнах
        print(f"Counting words with window={window_duration_seconds}s, slide={slide_duration_seconds}s...")
        word_counts = count_words_in_sliding_window(
            parsed,
            window_duration_seconds,
            slide_duration_seconds
        )
        
        # Выводим результаты в консоль
        print("Starting streaming query. Output will appear in console...")
        print("Press Ctrl+C to stop.")
        print("-" * 60)
        
        query = write_output_to_console(word_counts)
        
        # Ожидаем завершения (бесконечно, пока не будет прервано)
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStreaming query stopped by user.")
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")
        sys.exit(1)
    finally:
        print("Stopping SparkSession...")
        spark.stop()
        print("Done.")


if __name__ == "__main__":
    main()
