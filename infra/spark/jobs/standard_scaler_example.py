#!/usr/bin/env python3
"""
PySpark StandardScaler Example
Использует pyspark.ml.feature.StandardScaler для масштабирования признаков.
Готов к запуску через spark-submit.
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col


def create_spark_session(app_name="StandardScalerExample"):
    """Создание SparkSession."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_sample_data(spark):
    """Создание примера данных для масштабирования."""
    data = [
        (0.0, 1.0, 100.0),
        (1.0, 2.0, 200.0),
        (2.0, 3.0, 300.0),
        (3.0, 4.0, 400.0),
        (4.0, 5.0, 500.0),
        (5.0, 6.0, 600.0),
        (6.0, 7.0, 700.0),
        (7.0, 8.0, 800.0),
        (8.0, 9.0, 900.0),
        (9.0, 10.0, 1000.0),
    ]
    columns = ["feature1", "feature2", "feature3"]
    df = spark.createDataFrame(data, columns)
    return df


def apply_standard_scaler(df, input_cols, output_col="scaled_features"):
    """
    Применение StandardScaler к признакам.
    
    StandardScaler стандартизирует признаки, вычитая среднее и деля на стандартное отклонение.
    Результат: признаки с нулевым средним и единичной дисперсией.
    """
    # Векторизация признаков
    assembler = VectorAssembler(
        inputCols=input_cols,
        outputCol="features"
    )
    
    # Настройка StandardScaler
    scaler = StandardScaler(
        inputCol="features",
        outputCol=output_col,
        withStd=True,   # Делить на стандартное отклонение
        withMean=True   # Вычитать среднее
    )
    
    # Создание пайплайна
    pipeline = Pipeline(stages=[assembler, scaler])
    
    # Обучение и трансформация
    model = pipeline.fit(df)
    scaled_df = model.transform(df)
    
    return scaled_df, model


def main():
    """Основная функция."""
    # Создание SparkSession
    spark = create_spark_session()
    
    try:
        # Создание примера данных
        print("Создание примера данных...")
        df = create_sample_data(spark)
        print("Исходные данные:")
        df.show(truncate=False)
        
        # Применение StandardScaler
        input_columns = ["feature1", "feature2", "feature3"]
        print(f"\nПрименение StandardScaler к признакам: {input_columns}")
        
        scaled_df, model = apply_standard_scaler(df, input_columns)
        
        print("\nДанные после масштабирования:")
        scaled_df.select(
            col("feature1"),
            col("feature2"),
            col("feature3"),
            col("scaled_features")
        ).show(truncate=False)
        
        # Извлечение статистики скалера
        print("\nСтатистика StandardScaler:")
        scaled_features = model.stages[1]  # StandardScalerModel
        print(f"  mean: {scaled_features.mean}")
        print(f"  std: {scaled_features.std}")
        
        # Сохранение результатов (опционально)
        # scaled_df.write.mode("overwrite").parquet("/output/scaled_data")
        
        print("\nStandardScaler пример успешно выполнен!")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
