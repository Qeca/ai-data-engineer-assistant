#!/usr/bin/env python3
"""
Spark Structured Streaming: Word Count with Sliding Window

Подсчитывает слова в UTF8 тексте, разделенном '\n', поступающем из сети.
Каждая строка имеет временную метку для определения скользящих окон.
Окна имеют настраиваемую длительность.

Запуск:
    spark-submit network_word_count_sliding_window.py <hostname> <port> <window_duration> <slide_duration>

Пример:
    spark-submit network_word_count_sliding_window.py localhost 9999 10 seconds 5 seconds
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
    lit
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


def create_spark_session(app_name="NetworkWordCountSlidingWindow"):
    """Создает Spark сессию для Structured Streaming."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    # Уменьшаем уровень логирования для чистоты вывода
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_stream_from_socket(spark, hostname, port):
    """
    Читает поток данных из сетевого сокета.
    Каждая строка ожидается в формате: timestamp|text
    где timestamp - Unix timestamp в секундах.
    """
    lines = spark.readStream \
        .format("socket") \
        .option("host", hostname) \
        .option("port", port) \
        .load()
    
    return lines


def parse_lines_with_timestamp(lines):
    """
    Парсит строки формата: timestamp|text
    Добавляет колонку event_time как TimestampType.
    """
    # Разделяем строку на timestamp и текст
    parsed = lines.select(
        split(col("value"), "\\|", 2).alias("parts")
    ).select(
        col("parts").getItem(0).alias("timestamp_str"),
        col("parts").getItem(1).alias("text")
    )
    
    # Преобразуем timestamp строку в Unix timestamp и затем в TimestampType
    parsed = parsed.withColumn(
        "event_time",
        from_unixtime(col("timestamp_str").cast("double"))
    ).drop("timestamp_str")
    
    return parsed


def parse_lines_auto_timestamp(lines):
    """
    Альтернативный парсер: если данные без явного timestamp,
    использует время получения строки как event_time.
    """
    parsed = lines.select(
        col("value").alias("text"),
        current_timestamp().alias("event_time")
    )
    return parsed


def count_words_in_sliding_window(df, window_duration, slide_duration):
    """
    Применяет скользящее окно и подсчитывает слова.
    
    Args:
        df: DataFrame с колонками 'text' и 'event_time'
        window_duration: длительность окна (например, "10 seconds")
        slide_duration: шаг скольжения окна (например, "5 seconds")
    
    Returns:
        DataFrame с колонками: window, word, count
    """
    # Разбиваем текст на слова
    words = df.select(
        explode(split(col("text"), "\\s+")).alias("word"),
        col("event_time")
    )
    
    # Применяем скользящее окно и группируем
    windowed_counts = words \
        .groupBy(
            window(col("event_time"), window_duration, slide_duration),
            col("word")
        ) \
        .count() \
        .orderBy(col("window"), col("count").desc())
    
    return windowed_counts


def write_output_to_console(df):
    """Выводит результаты в консоль в режиме append."""
    query = df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    return query


def write_output_to_memory(df, spark):
    """
    Выводит результаты в память для программного доступа.
    Полезно для тестирования.
    """
    query = df.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("word_counts") \
        .start()
    
    return query


def main():
    # Парсим аргументы командной строки
    if len(sys.argv) < 5:
        print("Usage: network_word_count_sliding_window.py <hostname> <port> <window_duration> <slide_duration>")
        print("Example: network_word_count_sliding_window.py localhost 9999 10 seconds 5 seconds")
        print("  window_duration: длительность окна (например, '10 seconds', '1 minute')")
        print("  slide_duration: шаг скольжения (например, '5 seconds', '30 seconds')")
        sys.exit(1)
    
    hostname = sys.argv[1]
    port = int(sys.argv[2])
    window_duration = f"{sys.argv[3]} {sys.argv[4]}"
    slide_duration = f"{sys.argv[5]} {sys.argv[6]}" if len(sys.argv) > 6 else window_duration
    
    print(f"Starting Network Word Count with Sliding Window...")
    print(f"  Host: {hostname}")
    print(f"  Port: {port}")
    print(f"  Window Duration: {window_duration}")
    print(f"  Slide Duration: {slide_duration}")
    
    # Создаем Spark сессию
    spark = create_spark_session()
    
    try:
        # Читаем поток из сокета
        lines = read_stream_from_socket(spark, hostname, port)
        
        # Парсим строки с временными метками
        # Ожидаем формат: timestamp|text (например: 1699900000|hello world)
        parsed = parse_lines_with_timestamp(lines)
        
        # Подсчитываем слова в скользящих окнах
        word_counts = count_words_in_sliding_window(
            parsed, 
            window_duration, 
            slide_duration
        )
        
        # Выводим результаты в консоль
        query = write_output_to_console(word_counts)
        
        print("Streaming query started. Waiting for termination...")
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
