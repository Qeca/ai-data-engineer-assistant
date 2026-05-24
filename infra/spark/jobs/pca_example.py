#!/usr/bin/env python3
"""
PySpark PCA Example Script
Использует pyspark.ml.feature и pyspark.ml.linalg для построения PCA модели.
Готов к запуску через spark-submit.
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, DoubleType


def create_spark_session(app_name="PCA Example"):
    """Создание Spark сессии."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_sample_data(spark):
    """Создание демонстрационных данных для PCA."""
    data = [
        (0, Vectors.dense([1.0, 2.0, 3.0, 4.0, 5.0])),
        (1, Vectors.dense([2.0, 3.0, 4.0, 5.0, 6.0])),
        (2, Vectors.dense([3.0, 4.0, 5.0, 6.0, 7.0])),
        (3, Vectors.dense([4.0, 5.0, 6.0, 7.0, 8.0])),
        (4, Vectors.dense([5.0, 6.0, 7.0, 8.0, 9.0])),
        (5, Vectors.dense([10.0, 11.0, 12.0, 13.0, 14.0])),
        (6, Vectors.dense([11.0, 12.0, 13.0, 14.0, 15.0])),
        (7, Vectors.dense([12.0, 13.0, 14.0, 15.0, 16.0])),
        (8, Vectors.dense([13.0, 14.0, 15.0, 16.0, 17.0])),
        (9, Vectors.dense([14.0, 15.0, 16.0, 17.0, 18.0])),
    ]
    
    df = spark.createDataFrame(data, ["id", "features"])
    return df


def build_pca_model(df, k=2):
    """
    Построение PCA модели.
    
    Args:
        df: DataFrame с колонкой 'features' типа Vector
        k: количество главных компонент
    
    Returns:
        PCA модель и трансформированный DataFrame
    """
    # Стандартизация данных перед PCA (рекомендуется)
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                           withStd=True, withMean=False)
    scaler_model = scaler.fit(df)
    scaled_df = scaler_model.transform(df)
    
    # Создание и обучение PCA модели
    pca = PCA(k=k, inputCol="scaledFeatures", outputCol="pcaFeatures")
    pca_model = pca.fit(scaled_df)
    
    # Трансформация данных
    result_df = pca_model.transform(scaled_df)
    
    return pca_model, result_df


def print_pca_results(pca_model, result_df):
    """Вывод результатов PCA."""
    print("\n" + "="*60)
    print("PCA Результаты")
    print("="*60)
    
    # Объясненная дисперсия
    print(f"\nОбъясненная дисперсия каждой компоненты:")
    for i, variance in enumerate(pca_model.explainedVariance.toArray()):
        print(f"  Компонента {i+1}: {variance:.4f}")
    
    # Главные компоненты (веса)
    print(f"\nГлавные компоненты (PC weights):")
    pc_matrix = pca_model.pc
    for i in range(pc_matrix.numRows):
        weights = pc_matrix.toArray()[i]
        print(f"  PC{i+1}: {[round(w, 4) for w in weights]}")
    
    # Пример трансформированных данных
    print(f"\nПример трансформированных данных (первые 5 строк):")
    result_df.select("id", "pcaFeatures").show(5, truncate=False)
    
    print("="*60 + "\n")


def main():
    """Основная функция скрипта."""
    # Создание Spark сессии
    spark = create_spark_session("PCA Example with pyspark.ml")
    
    try:
        # Создание демонстрационных данных
        print("Создание демонстрационных данных...")
        df = create_sample_data(spark)
        print(f"Исходные данные: {df.count()} строк")
        df.show(5, truncate=False)
        
        # Построение PCA модели с 2 главными компонентами
        print("\nПостроение PCA модели (k=2)...")
        pca_model, result_df = build_pca_model(df, k=2)
        
        # Вывод результатов
        print_pca_results(pca_model, result_df)
        
        # Сохранение модели (опционально)
        # pca_model.write().overwrite().save("/tmp/pca_model")
        # print("Модель сохранена в /tmp/pca_model")
        
    except Exception as e:
        print(f"Ошибка выполнения: {e}")
        raise
    
    finally:
        spark.stop()
        print("Spark сессия завершена.")


if __name__ == "__main__":
    main()
