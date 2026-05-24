#!/usr/bin/env python3
"""
Decision Tree Classification Example using PySpark ML
Модули: pyspark.ml.evaluation, pyspark.ml.classification, pyspark.ml.feature
Ключевые классы: MulticlassClassificationEvaluator, StringIndexer, Pipeline, 
                 VectorIndexer, DecisionTreeClassifier
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def main():
    # Создание SparkSession
    spark = SparkSession.builder \
        .appName("DecisionTreeClassificationExample") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Создание примера данных для классификации
    # Данные: признаки (features) и целевая переменная (label)
    data = [
        (0, 1.0, 2.0, 3.0, "low"),
        (1, 4.0, 5.0, 6.0, "medium"),
        (0, 7.0, 8.0, 9.0, "high"),
        (1, 10.0, 11.0, 12.0, "low"),
        (0, 13.0, 14.0, 15.0, "medium"),
        (1, 16.0, 17.0, 18.0, "high"),
        (0, 19.0, 20.0, 21.0, "low"),
        (1, 22.0, 23.0, 24.0, "medium"),
        (0, 25.0, 26.0, 27.0, "high"),
        (1, 28.0, 29.0, 30.0, "low"),
        (0, 31.0, 32.0, 33.0, "medium"),
        (1, 34.0, 35.0, 36.0, "high"),
        (0, 37.0, 38.0, 39.0, "low"),
        (1, 40.0, 41.0, 42.0, "medium"),
        (0, 43.0, 44.0, 45.0, "high"),
        (1, 46.0, 47.0, 48.0, "low"),
        (0, 49.0, 50.0, 51.0, "medium"),
        (1, 52.0, 53.0, 54.0, "high"),
        (0, 55.0, 56.0, 57.0, "low"),
        (1, 58.0, 59.0, 60.0, "medium"),
    ]
    
    columns = ["id", "feature1", "feature2", "feature3", "label"]
    df = spark.createDataFrame(data, columns)
    
    print("=== Исходные данные ===")
    df.show(10)
    print(f"Всего записей: {df.count()}")
    
    # StringIndexer для кодирования строковой целевой переменной
    labelIndexer = StringIndexer(
        inputCol="label",
        outputCol="indexedLabel"
    )
    
    # VectorAssembler для объединения признаков в вектор
    featureAssembler = VectorAssembler(
        inputCols=["feature1", "feature2", "feature3"],
        outputCol="features"
    )
    
    # VectorIndexer для обработки категориальных признаков в векторе
    # maxCategories=4 означает, что признаки с <=4 уникальными значениями 
    # будут считаться категориальными
    featureIndexer = VectorIndexer(
        inputCol="features",
        outputCol="indexedFeatures",
        maxCategories=4
    )
    
    # DecisionTreeClassifier - модель классификации
    dt = DecisionTreeClassifier(
        featuresCol="indexedFeatures",
        labelCol="indexedLabel",
        predictionCol="prediction",
        maxDepth=5,
        maxBins=32,
        minInstancesPerNode=1,
        minInfoGain=0.0,
        checkpointInterval=10,
        impurity="gini"
    )
    
    # Создание Pipeline для последовательного применения трансформеров и модели
    pipeline = Pipeline(stages=[
        labelIndexer,
        featureAssembler,
        featureIndexer,
        dt
    ])
    
    # Разделение данных на train и test (70/30)
    trainData, testData = df.randomSplit([0.7, 0.3], seed=42)
    
    print(f"\n=== Разделение данных ===")
    print(f"Train set: {trainData.count()} записей")
    print(f"Test set: {testData.count()} записей")
    
    # Обучение модели
    print("\n=== Обучение модели Decision Tree ===")
    model = pipeline.fit(trainData)
    
    # Предсказания на тестовых данных
    predictions = model.transform(testData)
    
    print("\n=== Предсказания ===")
    predictions.select(
        "id", "label", "indexedLabel", "prediction"
    ).show(10)
    
    # Оценка модели с помощью MulticlassClassificationEvaluator
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel",
        predictionCol="prediction",
        metricName="accuracy"
    )
    
    accuracy = evaluator.evaluate(predictions)
    print(f"\n=== Метрики качества ===")
    print(f"Accuracy (Точность): {accuracy:.4f}")
    
    # Дополнительные метрики
    evaluator_precision = MulticlassClassificationEvaluator(
        labelCol="indexedLabel",
        predictionCol="prediction",
        metricName="weightedPrecision"
    )
    precision = evaluator_precision.evaluate(predictions)
    print(f"Weighted Precision: {precision:.4f}")
    
    evaluator_recall = MulticlassClassificationEvaluator(
        labelCol="indexedLabel",
        predictionCol="prediction",
        metricName="weightedRecall"
    )
    recall = evaluator_recall.evaluate(predictions)
    print(f"Weighted Recall: {recall:.4f}")
    
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="indexedLabel",
        predictionCol="prediction",
        metricName="f1"
    )
    f1 = evaluator_f1.evaluate(predictions)
    print(f"Weighted F1 Score: {f1:.4f}")
    
    # Отображение структуры модели Decision Tree
    print("\n=== Структура модели Decision Tree ===")
    dtModel = model.stages[-1]  # Последний этап pipeline - это модель DT
    print(dtModel.toDebugString())
    
    # Важность признаков
    print("\n=== Важность признаков ===")
    featureImportance = dtModel.featureImportances
    print(f"Feature 1: {featureImportance[0]:.4f}")
    print(f"Feature 2: {featureImportance[1]:.4f}")
    print(f"Feature 3: {featureImportance[2]:.4f}")
    
    # Сохранение модели (опционально)
    # model.write().overwrite().save("/tmp/dt_classification_model")
    
    spark.stop()
    print("\n=== Выполнение завершено ===")


if __name__ == "__main__":
    main()
