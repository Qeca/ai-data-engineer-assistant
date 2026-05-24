#!/usr/bin/env python3
"""
Word2Vec Example using PySpark ML
Готов к запуску через spark-submit
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, Word2Vec
from pyspark.sql.functions import col, explode


def create_spark_session(app_name="Word2VecExample"):
    """Создание SparkSession"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def prepare_sample_data(spark):
    """Подготовка примера данных для обучения"""
    documents = [
        ("doc1", "Apache Spark is a fast general-purpose cluster computing system"),
        ("doc2", "Spark provides high-level APIs in Java Scala Python and R"),
        ("doc3", "Spark supports SQL streaming machine learning and graph processing"),
        ("doc4", "Machine learning algorithms are built on top of Spark"),
        ("doc5", "Deep learning can be integrated with Spark MLlib"),
        ("doc6", "Word2Vec is a word embedding technique for NLP"),
        ("doc7", "Natural language processing uses word embeddings"),
        ("doc8", "PySpark ML provides machine learning pipelines"),
    ]
    
    df = spark.createDataFrame(documents, ["id", "text"])
    return df


def build_word2vec_pipeline(spark, df):
    """Построение пайплайна Word2Vec"""
    
    # Токенизация текста
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    tokenized_df = tokenizer.transform(df)
    
    # Обучение модели Word2Vec
    word2vec = Word2Vec(
        inputCol="words",
        outputCol="vectors",
        vectorSize=100,
        minCount=1,
        numPartitions=2,
        seed=42
    )
    
    model = word2vec.fit(tokenized_df)
    
    return model, tokenized_df


def find_similar_words(model, word, top_n=5):
    """Поиск похожих слов"""
    try:
        synonyms = model.findSynonyms(word, top_n)
        return synonyms
    except Exception as e:
        print(f"Word '{word}' not found in vocabulary: {e}")
        return None


def main():
    """Основная функция"""
    # Создание SparkSession
    spark = create_spark_session()
    
    try:
        # Подготовка данных
        print("Подготовка данных...")
        df = prepare_sample_data(spark)
        df.show(truncate=False)
        
        # Обучение модели Word2Vec
        print("\nОбучение модели Word2Vec...")
        model, tokenized_df = build_word2vec_pipeline(spark, df)
        
        # Применение модели к данным
        print("\nВекторизация документов...")
        result_df = model.transform(tokenized_df)
        result_df.select("id", "text", "vectors").show(truncate=False)
        
        # Поиск похожих слов
        print("\nПоиск похожих слов...")
        test_words = ["spark", "machine", "learning", "word"]
        
        for word in test_words:
            print(f"\nСинонимы для '{word}':")
            synonyms = find_similar_words(model, word)
            if synonyms:
                synonyms.show()
        
        # Сохранение модели
        model_path = "/tmp/word2vec_model"
        print(f"\nСохранение модели в {model_path}...")
        model.write().overwrite().save(model_path)
        
        # Загрузка модели (проверка)
        print("Загрузка сохраненной модели...")
        loaded_model = Word2Vec.load(model_path)
        
        # Статистика модели
        print("\nСтатистика модели:")
        print(f"Размер вектора: {model.getVectorSize()}")
        print(f"Минимальная частота слова: {model.getMinCount()}")
        
        # Вывод словаря
        vocabulary = model.getVectors()
        print(f"\nРазмер словаря: {vocabulary.count()} слов")
        vocabulary.show(truncate=False)
        
        print("\nWord2Vec пример успешно завершен!")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
