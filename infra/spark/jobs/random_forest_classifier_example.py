#!/usr/bin/env python3
"""
Random Forest Classifier Example using PySpark ML
Готов к запуску через spark-submit
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorIndexer, IndexToString


def main():
    # Создание SparkSession
    spark = SparkSession.builder \
        .appName("RandomForestClassifierExample") \
        .getOrCreate()
    
    # Загрузка примера данных (iris dataset или синтетические данные)
    # Для демонстрации создадим синтетический датасет
    data = [
        (0, 5.1, 3.5, 1.4, 0.2, "setosa"),
        (0, 4.9, 3.0, 1.4, 0.2, "setosa"),
        (0, 4.7, 3.2, 1.3, 0.2, "setosa"),
        (1, 7.0, 3.2, 4.7, 1.4, "versicolor"),
        (1, 6.4, 3.2, 4.5, 1.5, "versicolor"),
        (1, 6.9, 3.1, 4.9, 1.5, "versicolor"),
        (2, 6.3, 3.3, 6.0, 2.5, "virginica"),
        (2, 5.8, 2.7, 5.1, 1.9, "virginica"),
        (2, 7.1, 3.0, 5.9, 2.1, "virginica"),
    ]
    
    columns = ["id", "sepal_length", "sepal_width", "petal_length", "petal_width", "label"]
    df = spark.createDataFrame(data, columns)
    
    print("Исходные данные:")
    df.show()
    
    # StringIndexer для кодирования меток (label) в числовые значения
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel")
    
    # VectorIndexer для автоматического определения категориальных признаков
    # maxCategories=4 означает, что признаки с <=4 уникальными значениями считаются категориальными
    featureIndexer = VectorIndexer(
        inputCol="features", 
        outputCol="indexedFeatures", 
        maxCategories=4
    )
    
    # Подготовка признаков: объединение числовых колонок в вектор
    from pyspark.ml.feature import VectorAssembler
    assembler = VectorAssembler(
        inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        outputCol="features"
    )
    
    # RandomForestClassifier
    rf = RandomForestClassifier(
        labelCol="indexedLabel",
        featuresCol="indexedFeatures",
        numTrees=10,
        maxDepth=3,
        seed=42
    )
    
    # IndexToString для преобразования предсказанных индексов обратно в оригинальные метки
    labelConverter = IndexToString(
        inputCol="prediction",
        outputCol="predictedLabel",
        labels=labelIndexer.labels
    )
    
    # Создание Pipeline
    pipeline = Pipeline(stages=[
        labelIndexer,
        assembler,
        featureIndexer,
        rf,
        labelConverter
    ])
    
    # Разделение данных на train и test
    trainData, testData = df.randomSplit([0.7, 0.3], seed=42)
    
    print(f"Train set size: {trainData.count()}")
    print(f"Test set size: {testData.count()}")
    
    # Обучение модели
    model = pipeline.fit(trainData)
    
    # Предсказания на тестовых данных
    predictions = model.transform(testData)
    
    print("\nПредсказания:")
    predictions.select("label", "predictedLabel", "probability").show(truncate=False)
    
    # Оценка модели
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel",
        predictionCol="prediction",
        metricName="accuracy"
    )
    
    accuracy = evaluator.evaluate(predictions)
    print(f"\nAccuracy = {accuracy * 100:.2f}%")
    
    # Дополнительные метрики
    evaluatorPrecision = MulticlassClassificationEvaluator(
        labelCol="indexedLabel",
        predictionCol="prediction",
        metricName="weightedPrecision"
    )
    evaluatorRecall = MulticlassClassificationEvaluator(
        labelCol="indexedLabel",
        predictionCol="prediction",
        metricName="weightedRecall"
    )
    evaluatorF1 = MulticlassClassificationEvaluator(
        labelCol="indexedLabel",
        predictionCol="prediction",
        metricName="f1"
    )
    
    print(f"Weighted Precision = {evaluatorPrecision.evaluate(predictions):.4f}")
    print(f"Weighted Recall = {evaluatorRecall.evaluate(predictions):.4f}")
    print(f"F1 Score = {evaluatorF1.evaluate(predictions):.4f}")
    
    # Важность признаков (feature importance)
    rfModel = model.stages[3]  # RandomForestClassifier находится на 4-й позиции в pipeline
    print("\nВажность признаков:")
    for i, importance in enumerate(rfModel.featureImportances):
        print(f"  Feature {i}: {importance:.4f}")
    
    spark.stop()
    print("\nСкрипт выполнен успешно!")


if __name__ == "__main__":
    main()
