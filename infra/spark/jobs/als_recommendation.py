#!/usr/bin/env python3
"""
PySpark ALS Recommendation Model Script
Использует pyspark.ml.recommendation.ALS для построения модели рекомендаций
"""

from pyspark.sql import SparkSession, Row
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col


def create_spark_session(app_name="ALS Recommendation"):
    """Создание SparkSession"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_sample_data(spark):
    """Создание примера данных для обучения модели"""
    # Данные: userId, itemId, rating, timestamp
    ratings_data = [
        Row(userId=0, itemId=0, rating=4.0, timestamp=1000),
        Row(userId=0, itemId=1, rating=2.0, timestamp=1001),
        Row(userId=0, itemId=2, rating=5.0, timestamp=1002),
        Row(userId=1, itemId=0, rating=3.0, timestamp=1003),
        Row(userId=1, itemId=1, rating=4.0, timestamp=1004),
        Row(userId=1, itemId=2, rating=1.0, timestamp=1005),
        Row(userId=2, itemId=0, rating=5.0, timestamp=1006),
        Row(userId=2, itemId=1, rating=3.0, timestamp=1007),
        Row(userId=2, itemId=2, rating=4.0, timestamp=1008),
        Row(userId=3, itemId=0, rating=2.0, timestamp=1009),
        Row(userId=3, itemId=1, rating=5.0, timestamp=1010),
        Row(userId=3, itemId=2, rating=3.0, timestamp=1011),
    ]
    
    ratings = spark.createDataFrame(ratings_data)
    return ratings


def train_als_model(ratings, rank=10, maxIter=10, regParam=0.1, userCol="userId", 
                    itemCol="itemId", ratingCol="rating"):
    """
    Обучение модели ALS
    
    Parameters:
    -----------
    ratings : DataFrame
        DataFrame с колонками userId, itemId, rating
    rank : int
        Ранг матрицы (число скрытых факторов)
    maxIter : int
        Максимальное число итераций
    regParam : float
        Параметр регуляризации
    """
    als = ALS(
        rank=rank,
        maxIter=maxIter,
        regParam=regParam,
        userCol=userCol,
        itemCol=itemCol,
        ratingCol=ratingCol,
        coldStartStrategy="drop"  # Обработка холодного старта
    )
    
    model = als.fit(ratings)
    return model


def evaluate_model(model, test_data, ratingCol="rating"):
    """
    Оценка модели с помощью RegressionEvaluator
    
    Returns:
    --------
    float : RMSE (Root Mean Square Error)
    """
    predictions = model.transform(test_data)
    
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol=ratingCol,
        predictionCol="prediction"
    )
    
    rmse = evaluator.evaluate(predictions)
    return rmse


def get_recommendations(model, spark, num_users=5, num_items=3):
    """
    Получение рекомендаций для пользователей
    
    Returns:
    --------
    DataFrame : Топ-N рекомендаций для каждого пользователя
    """
    # Генерация рекомендаций для всех пользователей
    user_rec = model.recommendForAllUsers(num_items)
    
    # Генерация рекомендаций для всех items
    item_rec = model.recommendForAllItems(num_users)
    
    return user_rec, item_rec


def main():
    """Основная функция выполнения скрипта"""
    print("=" * 60)
    print("PySpark ALS Recommendation Model")
    print("=" * 60)
    
    # Создание SparkSession
    spark = create_spark_session()
    
    try:
        # Создание данных
        print("\n[1] Создание примера данных...")
        ratings = create_sample_data(spark)
        ratings.show()
        print(f"Всего записей: {ratings.count()}")
        
        # Разделение на train/test (80/20)
        print("\n[2] Разделение данных на train/test...")
        train_data, test_data = ratings.randomSplit([0.8, 0.2], seed=42)
        print(f"Train: {train_data.count()}, Test: {test_data.count()}")
        
        # Обучение модели
        print("\n[3] Обучение модели ALS...")
        model = train_als_model(train_data, rank=10, maxIter=15, regParam=0.05)
        print("Модель успешно обучена!")
        
        # Оценка модели
        print("\n[4] Оценка модели...")
        if test_data.count() > 0:
            rmse = evaluate_model(model, test_data)
            print(f"RMSE на тестовых данных: {rmse:.4f}")
        else:
            # Если тестовых данных мало, оцениваем на train
            rmse = evaluate_model(model, train_data)
            print(f"RMSE на обучающих данных: {rmse:.4f}")
        
        # Получение рекомендаций
        print("\n[5] Генерация рекомендаций...")
        user_rec, item_rec = get_recommendations(model, spark)
        
        print("\nТоп-3 рекомендации для пользователей:")
        user_rec.show(truncate=False)
        
        print("\nТоп-5 рекомендаций для товаров:")
        item_rec.show(truncate=False)
        
        # Пример предсказания для конкретных user-item пар
        print("\n[6] Пример предсказаний для конкретных пар user-item...")
        sample_pairs = spark.createDataFrame([
            Row(userId=0, itemId=0),
            Row(userId=1, itemId=1),
            Row(userId=2, itemId=2),
        ])
        
        predictions = model.transform(sample_pairs)
        predictions.show()
        
        print("\n" + "=" * 60)
        print("Скрипт выполнен успешно!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nОшибка выполнения: {e}")
        raise
    
    finally:
        spark.stop()
        print("\nSparkSession остановлен.")


if __name__ == "__main__":
    main()
