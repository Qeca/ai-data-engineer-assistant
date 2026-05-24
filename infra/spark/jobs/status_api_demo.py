#!/usr/bin/env python3
"""
PySpark Status API Demo Script
Использует SparkContext, Queue, Thread, SparkConf для многопоточной обработки.
Готов к запуску через spark-submit.
"""

import sys
import time
import threading
from queue import Queue, Empty
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def worker_thread(queue, results, worker_id):
    """
    Worker-поток для обработки задач из очереди.
    """
    while True:
        try:
            task = queue.get(timeout=1.0)
            if task is None:
                # Сигнал завершения
                queue.task_done()
                break
            
            # Имитация обработки задачи
            result = f"Worker-{worker_id} processed: {task}"
            results.append(result)
            print(f"[Thread-{worker_id}] {result}")
            
            queue.task_done()
        except Empty:
            continue
        except Exception as e:
            print(f"[Thread-{worker_id}] Error: {e}")
            queue.task_done()


def main():
    """
    Основная функция скрипта.
    """
    # Настройка SparkConf
    conf = SparkConf() \
        .setAppName("StatusApiDemo") \
        .setMaster("local[*]") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "1g") \
        .set("spark.sql.shuffle.partitions", "4")
    
    # Создание SparkContext
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    
    # Создание SparkSession для работы с DataFrame API
    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()
    
    print("=" * 60)
    print("Status API Demo - PySpark Script")
    print("=" * 60)
    print(f"Spark Version: {sc.version}")
    print(f"App Name: {sc.appName}")
    print(f"Master: {sc.master}")
    print("=" * 60)
    
    # Создание очереди и списка результатов
    task_queue = Queue()
    results = []
    
    # Количество worker-потоков
    num_workers = 4
    
    # Запуск worker-потоков
    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=worker_thread, args=(task_queue, results, i))
        t.daemon = True
        t.start()
        threads.append(t)
    
    print(f"\nStarted {num_workers} worker threads")
    
    # Добавление задач в очередь
    sample_data = [f"task_{i}" for i in range(20)]
    for task in sample_data:
        task_queue.put(task)
    
    print(f"Added {len(sample_data)} tasks to queue")
    
    # Ожидание завершения всех задач
    task_queue.join()
    
    # Отправка сигналов завершения потокам
    for _ in range(num_workers):
        task_queue.put(None)
    
    # Ожидание завершения потоков
    for t in threads:
        t.join(timeout=5.0)
    
    print(f"\nProcessing complete. Total results: {len(results)}")
    
    # Демонстрация работы с RDD
    print("\n" + "=" * 60)
    print("RDD Operations Demo")
    print("=" * 60)
    
    data = list(range(1, 101))
    rdd = sc.parallelize(data, numSlices=4)
    
    # Примеры операций
    count = rdd.count()
    sum_val = rdd.sum()
    avg = rdd.mean()
    max_val = rdd.max()
    min_val = rdd.min()
    
    print(f"Count: {count}")
    print(f"Sum: {sum_val}")
    print(f"Average: {avg}")
    print(f"Max: {max_val}")
    print(f"Min: {min_val}")
    
    # Фильтрация и маппинг
    even_squares = rdd.filter(lambda x: x % 2 == 0).map(lambda x: x ** 2).collect()
    print(f"First 10 even squares: {even_squares[:10]}")
    
    # Демонстрация работы с DataFrame
    print("\n" + "=" * 60)
    print("DataFrame Operations Demo")
    print("=" * 60)
    
    df = spark.createDataFrame(
        [(i, f"user_{i}", i * 100) for i in range(1, 21)],
        ["id", "name", "score"]
    )
    
    print("Sample DataFrame:")
    df.show(10)
    
    print("DataFrame with score > 500:")
    df.filter(df.score > 500).show()
    
    print("Aggregated stats:")
    df.agg(
        {"score": "avg"},
        {"score": "max"},
        {"score": "min"}
    ).show()
    
    # Очистка ресурсов
    print("\n" + "=" * 60)
    print("Shutting down...")
    print("=" * 60)
    
    spark.stop()
    sc.stop()
    
    print("Done!")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
