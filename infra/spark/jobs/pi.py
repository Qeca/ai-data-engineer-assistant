#!/usr/bin/env python3
"""
PySpark скрипт для вычисления числа π методом Монте-Карло.

Запуск через spark-submit:
    spark-submit --master local[*] pi.py [num_samples]

Аргументы:
    num_samples: количество случайных точек (по умолчанию 1000000)
"""

import sys
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, col, when, count


def estimate_pi(num_samples: int = 1_000_000) -> float:
    """
    Оценивает число π методом Монте-Карло.
    
    Алгоритм:
    1. Генерируем случайные точки (x, y) в единичном квадрате [0, 1] x [0, 1]
    2. Считаем, сколько точек попало в единичный круг (x^2 + y^2 <= 1)
    3. π ≈ 4 * (точки в круге) / (общее число точек)
    """
    # Создаём SparkSession
    spark = SparkSession.builder \
        .appName("Pi Estimation - Monte Carlo") \
        .getOrCreate()
    
    # Устанавливаем уровень логгирования
    spark.sparkContext.setLogLevel("WARN")
    
    # Генерируем DataFrame со случайными точками
    df = spark.range(num_samples) \
        .withColumn("x", rand()) \
        .withColumn("y", rand()) \
        .withColumn("in_circle", when(col("x")**2 + col("y")**2 <= 1, 1).otherwise(0))
    
    # Считаем количество точек внутри круга
    inside_count = df.agg(count(when(col("in_circle") == 1, 1)).alias("inside")).collect()[0]["inside"]
    
    # Вычисляем π
    pi_estimate = 4.0 * inside_count / num_samples
    
    # Выводим результат
    print(f"Количество сэмплов: {num_samples:,}")
    print(f"Точек внутри круга: {inside_count:,}")
    print(f"Оценка π: {pi_estimate:.10f}")
    print(f"Фактическое π: {3.141592653589793:.10f}")
    print(f"Погрешность: {abs(pi_estimate - 3.141592653589793):.10f}")
    
    spark.stop()
    
    return pi_estimate


if __name__ == "__main__":
    # Парсим аргументы командной строки
    num_samples = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    
    print(f"Запуск вычисления π методом Монте-Карло...")
    print(f"Используется {num_samples:,} случайных точек")
    print("-" * 50)
    
    pi_value = estimate_pi(num_samples)
    
    print("-" * 50)
    print(f"Результат: π ≈ {pi_value}")
