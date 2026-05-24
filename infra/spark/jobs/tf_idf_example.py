#!/usr/bin/env python3
"""
PySpark TF-IDF Example Script
Использует pyspark.ml.feature: Tokenizer, HashingTF, IDF
Готов к запуску через spark-submit
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def create_spark_session(app_name="TF-IDF-Example"):
    """Создание Spark сессии"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    # Создание Spark сессии
    spark = create_spark_session("TF-IDF-Example")
    
    # Пример данных: текст и метка класса
    data = [
        (0, "Spark is fast and scalable"),
        (1, "Machine learning with PySpark is powerful"),
        (0, "Fast data processing with Spark"),
        (1, "PySpark ML library for machine learning"),
        (0, "Spark streaming and batch processing"),
        (1, "Deep learning and neural networks with PySpark"),
    ]
    
    # Создание DataFrame
    columns = ["label", "text"]
    df = spark.createDataFrame(data, columns)
    
    print("=== Исходные данные ===")
    df.show(truncate=False)
    
    # Tokenizer: разбивает текст на токены (слова)
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    
    # HashingTF: преобразует токены в векторы признаков (TF - Term Frequency)
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
    
    # IDF: Inverse Document Frequency - взвешивание признаков
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    
    # Создание Pipeline
    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf])
    
    # Обучение pipeline и трансформация данных
    pipelineModel = pipeline.fit(df)
    tfidf_df = pipelineModel.transform(df)
    
    print("=== TF-IDF векторы ===")
    tfidf_df.select("text", "features").show(truncate=False)
    
    # Разделение данных на train/test
    train_data, test_data = tfidf_df.randomSplit([0.8, 0.2], seed=42)
    
    # Обучение модели логистической регрессии
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
    lrModel = lr.fit(train_data)
    
    # Предсказания
    predictions = lrModel.transform(test_data)
    
    print("=== Предсказания модели ===")
    predictions.select("text", "label", "prediction", "probability").show(truncate=False)
    
    # Оценка модели
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    print(f"=== Точность модели: {accuracy:.2f} ===")
    
    # Сохранение модели (опционально)
    # pipelineModel.write().overwrite().save("/tmp/tfidf_pipeline")
    # lrModel.write().overwrite().save("/tmp/lr_model")
    
    spark.stop()
    print("=== Скрипт завершен ===")


if __name__ == "__main__":
    main()
