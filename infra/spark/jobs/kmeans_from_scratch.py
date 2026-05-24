#!/usr/bin/env python3
"""
K-means clustering algorithm implemented from scratch using PySpark.
This script is ready for spark-submit.

Usage:
    spark-submit kmeans_from_scratch.py [data_path] [k] [max_iterations]
"""

import sys
import random
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType, IntegerType


def euclidean_distance(point1, point2):
    """Calculate Euclidean distance between two points."""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(point1, point2)))


def initialize_centroids(data, k, seed=42):
    """Initialize k centroids by randomly selecting k points from data."""
    random.seed(seed)
    points = data.collect()
    if len(points) < k:
        raise ValueError(f"Number of points ({len(points)}) is less than k ({k})")
    return random.sample(points, k)


def assign_clusters(point, centroids):
    """Assign a point to the nearest centroid."""
    min_distance = float('inf')
    cluster_id = 0
    for i, centroid in enumerate(centroids):
        distance = euclidean_distance(point, centroid)
        if distance < min_distance:
            min_distance = distance
            cluster_id = i
    return cluster_id


def compute_centroids(clustered_data, k, num_features):
    """Compute new centroids as the mean of points in each cluster."""
    new_centroids = []
    for cluster_id in range(k):
        cluster_points = clustered_data.filter(lambda x: x[1] == cluster_id).map(lambda x: x[0])
        count = cluster_points.count()
        if count == 0:
            # If cluster is empty, reinitialize with a random point
            new_centroids.append(clustered_data.map(lambda x: x[0]).first())
        else:
            # Compute mean for each feature
            sums = cluster_points.reduce(lambda a, b: tuple(x + y for x, y in zip(a, b)))
            mean = tuple(s / count for s in sums)
            new_centroids.append(mean)
    return new_centroids


def has_converged(old_centroids, new_centroids, tolerance=1e-4):
    """Check if centroids have converged."""
    for old, new in zip(old_centroids, new_centroids):
        if euclidean_distance(old, new) > tolerance:
            return False
    return True


def kmeans_spark(data_rdd, k, max_iterations=100, tolerance=1e-4):
    """
    K-means clustering algorithm using PySpark RDDs.
    
    Args:
        data_rdd: RDD of points (tuples of floats)
        k: Number of clusters
        max_iterations: Maximum number of iterations
        tolerance: Convergence tolerance
    
    Returns:
        Tuple of (centroids, clustered_data)
    """
    # Initialize centroids
    centroids = initialize_centroids(data_rdd, k)
    
    iteration = 0
    while iteration < max_iterations:
        # Assign each point to nearest centroid
        clustered_data = data_rdd.map(lambda point: (point, assign_clusters(point, centroids)))
        
        # Compute new centroids
        new_centroids = compute_centroids(clustered_data, k, len(centroids[0]))
        
        # Check convergence
        if has_converged(centroids, new_centroids, tolerance):
            print(f"Converged at iteration {iteration + 1}")
            break
        
        centroids = new_centroids
        iteration += 1
        print(f"Iteration {iteration}: centroids updated")
    
    # Final clustering
    clustered_data = data_rdd.map(lambda point: (point, assign_clusters(point, centroids)))
    
    return centroids, clustered_data


def generate_sample_data(spark, num_points=1000, num_features=2, k=3, seed=42):
    """Generate sample clustered data for testing."""
    random.seed(seed)
    
    # Generate k clusters with different centers
    centers = [(random.uniform(-10, 10), random.uniform(-10, 10)) for _ in range(k)]
    
    data = []
    for _ in range(num_points):
        cluster_idx = random.randint(0, k - 1)
        center = centers[cluster_idx]
        point = tuple(center[i] + random.gauss(0, 1) for i in range(num_features))
        data.append(point)
    
    return spark.sparkContext.parallelize(data)


def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("KMeansFromScratch") \
        .getOrCreate()
    
    # Parse command line arguments
    data_path = sys.argv[1] if len(sys.argv) > 1 else None
    k = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    max_iterations = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    
    print(f"K-means clustering with k={k}, max_iterations={max_iterations}")
    
    # Generate sample data or load from file
    if data_path:
        print(f"Loading data from {data_path}")
        # Load data from file (assuming CSV format with comma-separated values)
        data_rdd = spark.sparkContext.textFile(data_path) \
            .map(lambda line: tuple(float(x) for x in line.split(',')))
    else:
        print("Generating sample data...")
        data_rdd = generate_sample_data(spark, num_points=1000, num_features=2, k=k)
    
    # Run K-means
    print("Running K-means algorithm...")
    centroids, clustered_data = kmeans_spark(data_rdd, k, max_iterations)
    
    # Print results
    print("\n=== Final Centroids ===")
    for i, centroid in enumerate(centroids):
        print(f"Cluster {i}: {centroid}")
    
    # Count points per cluster
    print("\n=== Cluster Sizes ===")
    cluster_counts = clustered_data.map(lambda x: x[1]).countByValue()
    for cluster_id, count in sorted(cluster_counts.items()):
        print(f"Cluster {cluster_id}: {count} points")
    
    # Show sample clustered data
    print("\n=== Sample Clustered Data ===")
    samples = clustered_data.take(10)
    for point, cluster_id in samples:
        print(f"Point {point} -> Cluster {cluster_id}")
    
    # Save results (optional)
    # clustered_data.map(lambda x: (x[0], x[1])).saveAsTextFile("output/kmeans_result")
    
    spark.stop()
    print("\nK-means clustering completed successfully!")


if __name__ == "__main__":
    main()
