#!/usr/bin/env python3
"""
Simple example demonstrating model selection using CrossValidator.
This example also demonstrates how Pipelines are Estimators.

Run with: spark-submit cross_validator_example.py
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Tokenizer, HashingTF
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("CrossValidatorExample") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Prepare training data
    training = spark.createDataFrame([
        (0, "a b c d e spark", 1.0),
        (1, "b d", 0.0),
        (2, "spark f g h", 1.0),
        (3, "hadoop mapreduce", 0.0),
        (4, "b spark who", 1.0),
        (5, "g d a y", 0.0),
        (6, "spark fly", 1.0),
        (7, "was mapreduce", 0.0),
        (8, "e spark program", 1.0),
        (9, "a e c l", 0.0),
        (10, "spark compile", 1.0),
        (11, "hadoop software", 0.0)
    ], ["id", "text", "label"])
    
    # Configure an ML pipeline
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=1000)
    lr = LogisticRegression(maxIter=10, regParam=0.01)
    
    pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])
    
    # Use ParamGridBuilder to construct a grid of parameters to search over
    paramGrid = ParamGridBuilder() \
        .addGrid(hashingTF.numFeatures, [10, 100, 1000]) \
        .addGrid(lr.regParam, [0.1, 0.01]) \
        .build()
    
    # Use CrossValidator to select the best combination of hyperparameters
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=BinaryClassificationEvaluator(),
        numFolds=2
    )
    
    # Run cross-validation and choose the best set of parameters
    cvModel = cv.fit(training)
    
    # Get the best model
    bestModel = cvModel.bestModel
    
    print("=" * 60)
    print("CrossValidator Model Selection Example")
    print("=" * 60)
    print(f"\nBest model stages: {bestModel.stages}")
    print(f"Best hashingTF numFeatures: {bestModel.stages[1].getNumFeatures()}")
    print(f"Best logisticRegression regParam: {bestModel.stages[2].getRegParam()}")
    
    # Evaluate the best model on training data
    predictions = bestModel.transform(training)
    evaluator = BinaryClassificationEvaluator()
    areaUnderROC = evaluator.evaluate(predictions)
    
    print(f"\nArea under ROC on training data: {areaUnderROC:.4f}")
    
    # Show predictions
    print("\nPredictions sample:")
    predictions.select("id", "text", "label", "prediction", "probability").show(5, truncate=False)
    
    # Show all CV results
    print("\nCross-validation results:")
    for i, model in enumerate(cvModel.avgMetrics):
        print(f"  Model {i}: avgMetric = {model:.4f}")
    
    print(f"\nBest avgMetric: {cvModel.avgMetrics.max():.4f}")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
