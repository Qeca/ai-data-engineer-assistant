#!/usr/bin/env python3
"""
Gradient Boosted Tree Classifier Example using PySpark ML
Ready for spark-submit execution
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler


def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("GBTClassifierExample") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Load sample data - using built-in sample dataset
    # In production, replace with your data source
    data = spark.read.format("libsvm").load("sample_libsvm_data.txt")
    
    # Split data into training and test sets
    train_data, test_data = data.randomSplit([0.7, 0.3], seed=42)
    
    # Create VectorIndexer to handle categorical features
    # Features with <= 4 distinct values are treated as categorical
    vectorIndexer = VectorIndexer(
        inputCol="features",
        outputCol="indexedFeatures",
        maxCategories=4
    )
    
    # Create GBT Classifier
    gbt = GBTClassifier(
        featuresCol="indexedFeatures",
        labelCol="label",
        predictionCol="prediction",
        maxIter=10,
        maxDepth=3,
        stepSize=0.1
    )
    
    # Create Pipeline
    pipeline = Pipeline(stages=[vectorIndexer, gbt])
    
    # Train the model
    print("Training Gradient Boosted Tree model...")
    model = pipeline.fit(train_data)
    
    # Make predictions on test data
    print("Making predictions on test data...")
    predictions = model.transform(test_data)
    
    # Evaluate the model
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    
    accuracy = evaluator.evaluate(predictions)
    print(f"Test Accuracy = {accuracy:.4f}")
    
    # Additional metrics
    evaluator_precision = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedPrecision"
    )
    evaluator_recall = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedRecall"
    )
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )
    
    precision = evaluator_precision.evaluate(predictions)
    recall = evaluator_recall.evaluate(predictions)
    f1 = evaluator_f1.evaluate(predictions)
    
    print(f"Weighted Precision = {precision:.4f}")
    print(f"Weighted Recall = {recall:.4f}")
    print(f"F1 Score = {f1:.4f}")
    
    # Show sample predictions
    print("\nSample predictions:")
    predictions.select("label", "prediction", "probability").show(10, truncate=False)
    
    # Get feature importance from GBT model
    gbt_model = model.stages[1]
    feature_importance = gbt_model.featureImportances
    print(f"\nFeature Importance (first 10): {feature_importance[:10]}")
    
    spark.stop()
    print("\nJob completed successfully!")


if __name__ == "__main__":
    main()
