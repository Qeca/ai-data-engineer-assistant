#!/usr/bin/env python3
"""
PySpark Naive Bayes Classification Example
Использует pyspark.ml для построения модели классификации.
Готов к запуску через spark-submit.
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler


def create_spark_session(app_name="NaiveBayesExample"):
    """Создание Spark сессии."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_sample_data(spark):
    """
    Загрузка примера данных для классификации.
    В реальном сценарии замените на загрузку из файла/таблицы.
    """
    data = [
        (0, 1.0, 2.0, 3.0, 0),
        (1, 2.0, 3.0, 4.0, 1),
        (0, 3.0, 4.0, 5.0, 0),
        (1, 4.0, 5.0, 6.0, 1),
        (0, 5.0, 6.0, 7.0, 0),
        (1, 6.0, 7.0, 8.0, 1),
        (0, 7.0, 8.0, 9.0, 0),
        (1, 8.0, 9.0, 10.0, 1),
    ]
    columns = ["id", "feature1", "feature2", "feature3", "label"]
    df = spark.createDataFrame(data, columns)
    return df


def build_pipeline():
    """
    Построение ML пайплайна с использованием pyspark.ml.
    """
    # Векторизация признаков
    assembler = VectorAssembler(
        inputCols=["feature1", "feature2", "feature3"],
        outputCol="features"
    )
    
    # Модель Naive Bayes
    nb = NaiveBayes(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        probabilityCol="probability",
        smoothing=1.0
    )
    
    # Пайплайн
    pipeline = Pipeline(stages=[assembler, nb])
    return pipeline


def train_and_evaluate(spark, df, pipeline):
    """
    Обучение модели и оценка качества.
    """
    # Разделение данных на train/test
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    
    # Обучение модели
    model = pipeline.fit(train_df)
    
    # Предсказания на тестовых данных
    predictions = model.transform(test_df)
    
    # Оценка качества с использованием MulticlassClassificationEvaluator
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    
    accuracy = evaluator.evaluate(predictions)
    
    # Дополнительные метрики
    evaluator_precision = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedPrecision"
    )
    precision = evaluator_precision.evaluate(predictions)
    
    evaluator_recall = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedRecall"
    )
    recall = evaluator_recall.evaluate(predictions)
    
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )
    f1 = evaluator_f1.evaluate(predictions)
    
    return model, predictions, {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1
    }


def main():
    """Основная функция скрипта."""
    # Создание Spark сессии
    spark = create_spark_session()
    
    try:
        # Загрузка данных
        print("Загрузка данных...")
        df = load_sample_data(spark)
        df.show()
        
        # Построение пайплайна
        print("Построение ML пайплайна...")
        pipeline = build_pipeline()
        
        # Обучение и оценка
        print("Обучение модели Naive Bayes...")
        model, predictions, metrics = train_and_evaluate(spark, df, pipeline)
        
        # Вывод результатов
        print("\n=== Результаты предсказаний ===")
        predictions.select("label", "prediction", "probability").show()
        
        print("\n=== Метрики качества модели ===")
        print(f"Accuracy:  {metrics['accuracy']:.4f}")
        print(f"Precision: {metrics['precision']:.4f}")
        print(f"Recall:    {metrics['recall']:.4f}")
        print(f"F1 Score:  {metrics['f1']:.4f}")
        
        # Сохранение модели (опционально)
        # model.write().overwrite().save("/path/to/save/model")
        
        print("\nСкрипт выполнен успешно!")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
