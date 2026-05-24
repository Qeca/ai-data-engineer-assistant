#!/usr/bin/env python3
"""
PySpark ML Pipeline Example
Использует: Pipeline, Tokenizer, HashingTF, LogisticRegression
Готов к запуску через spark-submit
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def create_spark_session(app_name="ML Pipeline Example"):
    """Создание Spark сессии"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    # Инициализация Spark сессии
    spark = create_spark_session()
    
    # Пример данных для обучения
    training_data = spark.createDataFrame([
        (0, "a b c d e spark", 1.0),
        (1, "b d", 0.0),
        (2, "spark f g h", 1.0),
        (3, "hadoop mapreduce", 0.0),
        (4, "b spark who", 1.0),
        (5, "g d a y", 0.0),
        (6, "spark fly", 1.0),
        (7, "was mapreduce", 0.0),
        (8, "e spark program", 1.0),
        (9, "a e c l", 0.0)
    ], ["id", "text", "label"])
    
    print("=== Исходные данные ===")
    training_data.show(truncate=False)
    
    # Шаг 1: Tokenizer - разбивает текст на слова
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    
    # Шаг 2: HashingTF - преобразует слова в векторы признаков
    hashing_tf = HashingTF(inputCol="words", outputCol="features", numFeatures=1000)
    
    # Шаг 3: LogisticRegression - классификатор
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10, regParam=0.01)
    
    # Создание Pipeline
    pipeline = Pipeline(stages=[tokenizer, hashing_tf, lr])
    
    print("=== Обучение модели ===")
    # Обучение модели
    model = pipeline.fit(training_data)
    
    # Тестовые данные
    test_data = spark.createDataFrame([
        (4, "spark i j k"),
        (5, "l m n"),
        (6, "spark hadoop spark"),
        (7, "apache hadoop")
    ], ["id", "text"])
    
    print("=== Тестовые данные ===")
    test_data.show(truncate=False)
    
    # Предсказания
    predictions = model.transform(test_data)
    
    print("=== Предсказания ===")
    predictions.select("id", "text", "probability", "prediction").show(truncate=False)
    
    # Статистика предсказаний
    print("=== Статистика ===")
    predictions.groupBy("prediction").count().show()
    
    # Сохранение модели (опционально)
    # model.write().overwrite().save("/tmp/ml_pipeline_model")
    
    spark.stop()
    print("=== Выполнение завершено ===")


if __name__ == "__main__":
    main()
