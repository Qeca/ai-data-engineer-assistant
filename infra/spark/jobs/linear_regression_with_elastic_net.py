#!/usr/bin/env python3
"""
PySpark Linear Regression with Elastic Net
Использует pyspark.ml.regression.LinearRegression с параметрами Elastic Net
"""

from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import sys


def create_spark_session(app_name="LinearRegressionElasticNet"):
    """Создание SparkSession"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def generate_sample_data(spark, num_samples=10000, num_features=10):
    """Генерация синтетических данных для обучения"""
    from pyspark.sql.functions import rand, col
    
    # Генерация признаков
    columns = [f"feature_{i}" for i in range(num_features)]
    df = spark.range(num_samples)
    
    for i, col_name in enumerate(columns):
        df = df.withColumn(col_name, rand(seed=i) * 100)
    
    # Генерация целевой переменной с шумом
    # y = 2*x1 + 3*x2 - 1.5*x3 + noise
    df = df.withColumn(
        "label",
        2 * col("feature_0") + 
        3 * col("feature_1") - 
        1.5 * col("feature_2") + 
        (rand(seed=42) - 0.5) * 10
    )
    
    return df, columns


def prepare_features(df, feature_columns):
    """Подготовка признаков: VectorAssembler + StandardScaler"""
    # Объединение признаков в вектор
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features_raw"
    )
    
    # Стандартизация признаков
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=False
    )
    
    return assembler, scaler


def train_model(df, elastic_net_param=0.5, reg_param=0.1, max_iter=100):
    """
    Обучение модели Linear Regression с Elastic Net
    
    Параметры:
    - elastic_net_param: 0 = Ridge (L2), 1 = Lasso (L1), 0.5 = Elastic Net
    - reg_param: параметр регуляризации
    - max_iter: максимальное количество итераций
    """
    lr = LinearRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        elasticNetParam=elastic_net_param,  # Elastic Net параметр
        regParam=reg_param,                  # Параметр регуляризации
        maxIter=max_iter,
        tol=1e-6,
        fitIntercept=True,
        standardization=True
    )
    
    return lr


def evaluate_model(model, test_data):
    """Оценка качества модели"""
    predictions = model.transform(test_data)
    
    evaluator_rmse = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    evaluator_r2 = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="r2"
    )
    
    evaluator_mae = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="mae"
    )
    
    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    
    return {
        "rmse": rmse,
        "r2": r2,
        "mae": mae,
        "predictions": predictions
    }


def main():
    """Основная функция"""
    # Парсинг аргументов командной строки
    elastic_net_param = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5
    reg_param = float(sys.argv[2]) if len(sys.argv) > 2 else 0.1
    test_ratio = float(sys.argv[3]) if len(sys.argv) > 3 else 0.2
    
    print(f"Параметры модели:")
    print(f"  elasticNetParam: {elastic_net_param}")
    print(f"  regParam: {reg_param}")
    print(f"  test_ratio: {test_ratio}")
    
    # Создание SparkSession
    spark = create_spark_session()
    
    try:
        # Генерация данных
        print("Генерация синтетических данных...")
        df, feature_columns = generate_sample_data(spark, num_samples=10000, num_features=10)
        print(f"Сгенерировано {df.count()} записей с {len(feature_columns)} признаками")
        
        # Разделение на train/test
        train_data, test_data = df.randomSplit([1 - test_ratio, test_ratio], seed=42)
        print(f"Train: {train_data.count()}, Test: {test_data.count()}")
        
        # Подготовка признаков
        assembler, scaler = prepare_features(df, feature_columns)
        
        # Создание пайплайна
        lr_model = train_model(df, elastic_net_param=elastic_net_param, reg_param=reg_param)
        
        pipeline = Pipeline(stages=[assembler, scaler, lr_model])
        
        # Обучение модели
        print("Обучение модели...")
        model = pipeline.fit(train_data)
        
        # Оценка модели
        print("Оценка модели...")
        metrics = evaluate_model(model, test_data)
        
        print("\n" + "="*50)
        print("РЕЗУЛЬТАТЫ МОДЕЛИ")
        print("="*50)
        print(f"RMSE: {metrics['rmse']:.4f}")
        print(f"R2:   {metrics['r2']:.4f}")
        print(f"MAE:  {metrics['mae']:.4f}")
        print("="*50)
        
        # Получение коэффициентов модели
        lr_stage = model.stages[-1]
        coefficients = lr_stage.coefficients
        intercept = lr_stage.intercept
        
        print("\nКоэффициенты модели:")
        for i, (feat, coef) in enumerate(zip(feature_columns, coefficients)):
            print(f"  {feat}: {coef:.4f}")
        print(f"  Intercept: {intercept:.4f}")
        
        # Пример предсказаний
        print("\nПример предсказаний (первые 5):")
        metrics['predictions'].select("label", "prediction").show(5)
        
        # Сохранение модели (опционально)
        # model.write().overwrite().save("/tmp/linear_regression_elastic_net_model")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()
    
    print("\nСкрипт завершен успешно!")


if __name__ == "__main__":
    main()
