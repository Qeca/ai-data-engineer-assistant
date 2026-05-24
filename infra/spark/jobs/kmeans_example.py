"""
K-Means Clustering Example using PySpark ML

Run with:
  bin/spark-submit examples/src/main/python/ml/kmeans_example.py

This example demonstrates k-means clustering using pyspark.ml.clustering.KMeans
and evaluates the model using pyspark.ml.evaluation.ClusteringEvaluator.
"""

from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors


def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("KMeansExample") \
        .getOrCreate()

    # Create sample data with features for clustering
    # Each row represents a data point with 2 features
    data = [
        (Vectors.dense([0.0, 0.0]),),
        (Vectors.dense([0.1, 0.1]),),
        (Vectors.dense([0.2, 0.2]),),
        (Vectors.dense([10.0, 10.0]),),
        (Vectors.dense([10.1, 10.1]),),
        (Vectors.dense([10.2, 10.2]),),
        (Vectors.dense([20.0, 20.0]),),
        (Vectors.dense([20.1, 20.1]),),
        (Vectors.dense([20.2, 20.2]),),
        (Vectors.dense([30.0, 30.0]),),
        (Vectors.dense([30.1, 30.1]),),
        (Vectors.dense([30.2, 30.2]),),
    ]

    # Create DataFrame with features column
    dataset = spark.createDataFrame(data, ["features"])

    print("Input data:")
    dataset.show(truncate=False)

    # Configure KMeans clustering
    # k=4: number of clusters
    # featuresCol: input column with feature vectors
    # predictionCol: output column with cluster predictions
    kmeans = KMeans().setK(4).setSeed(1).setFeaturesCol("features").setPredictionCol("prediction")

    # Fit the model to the data
    model = kmeans.fit(dataset)

    # Make predictions
    predictions = model.transform(dataset)

    print("Predictions:")
    predictions.show(truncate=False)

    # Evaluate clustering using Silhouette Score
    # Range: [-1, 1], higher values indicate better clustering
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction")
    silhouette = evaluator.evaluate(predictions)

    print(f"Silhouette Score with k=4: {silhouette}")

    # Print cluster centers
    print("Cluster Centers:")
    centers = model.clusterCenters()
    for i, center in enumerate(centers):
        print(f"Center {i}: {center}")

    # Show cluster assignments with centers
    print("\nCluster assignments:")
    predictions.select("features", "prediction").orderBy("prediction").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
