#!/usr/bin/env python3
"""
PySpark скрипт для вычисления транзитивного замыкания графа.
Использует класс Random для генерации тестовых данных.
Готов к запуску через spark-submit.
"""

import sys
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast


def generate_random_edges(num_nodes, num_edges, seed=42):
    """
    Генерирует случайные рёбра графа с использованием класса Random.
    
    Args:
        num_nodes: количество узлов в графе
        num_edges: количество рёбер
        seed: seed для воспроизводимости
    
    Returns:
        Список кортежей (src, dst) представляющих рёбра
    """
    rng = random.Random(seed)
    edges = set()
    
    while len(edges) < num_edges:
        src = rng.randint(0, num_nodes - 1)
        dst = rng.randint(0, num_nodes - 1)
        if src != dst:  # Исключаем петли
            edges.add((src, dst))
    
    return list(edges)


def compute_transitive_closure(spark, edges_rdd):
    """
    Вычисляет транзитивное замыкание графа итеративным методом.
    
    Алгоритм:
    1. Начинаем с исходных рёбер
    2. На каждой итерации находим новые пути длиной +1
    3. Повторяем пока не перестанут появляться новые рёбра
    
    Args:
        spark: SparkSession
        edges_rdd: RDD кортежей (src, dst)
    
    Returns:
        DataFrame с транзитивным замыканием
    """
    # Создаем DataFrame из рёбер
    edges_df = spark.createDataFrame(edges_rdd, ["src", "dst"])
    
    # Инициализируем результат исходными рёбрами
    tc_df = edges_df
    
    # Максимальное количество итераций (защита от бесконечного цикла)
    max_iterations = 100
    
    for iteration in range(max_iterations):
        # Находим новые пути: (a,b) + (b,c) -> (a,c)
        # Join по dst первого = src второго
        new_paths = tc_df.alias("t1").join(
            edges_df.alias("e"),
            col("t1.dst") == col("e.src"),
            how="inner"
        ).select(
            col("t1.src").alias("src"),
            col("e.dst").alias("dst")
        )
        
        # Добавляем новые пути к результату, исключая дубликаты
        tc_before = tc_df.count()
        tc_df = tc_df.union(new_paths).distinct()
        tc_after = tc_df.count()
        
        print(f"Iteration {iteration + 1}: edges {tc_before} -> {tc_after}")
        
        # Если новых рёбер не добавилось, завершаем
        if tc_before == tc_after:
            print(f"Converged after {iteration + 1} iterations")
            break
    else:
        print(f"Warning: reached max iterations ({max_iterations})")
    
    return tc_df


def main():
    """
    Основная функция скрипта.
    """
    # Создаем SparkSession
    spark = SparkSession.builder \
        .appName("TransitiveClosureRandom") \
        .getOrCreate()
    
    # Параметры (можно передавать через spark-submit --conf)
    num_nodes = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    num_edges = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    seed = int(sys.argv[3]) if len(sys.argv) > 3 else 42
    
    print(f"Generating random graph: {num_nodes} nodes, {num_edges} edges, seed={seed}")
    
    # Генерируем случайные рёбра
    edges = generate_random_edges(num_nodes, num_edges, seed)
    edges_rdd = spark.sparkContext.parallelize(edges)
    
    print(f"Generated {len(edges)} unique edges")
    
    # Вычисляем транзитивное замыкание
    tc_df = compute_transitive_closure(spark, edges_rdd)
    
    # Показываем результаты
    print(f"\nTransitive closure contains {tc_df.count()} edges")
    print("\nSample of transitive closure:")
    tc_df.show(20, truncate=False)
    
    # Сохраняем результат (опционально)
    # tc_df.write.mode("overwrite").parquet("/output/transitive_closure")
    
    spark.stop()
    print("\nJob completed successfully!")


if __name__ == "__main__":
    main()
