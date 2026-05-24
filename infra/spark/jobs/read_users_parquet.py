#!/usr/bin/env python3
"""
Spark script to read users.parquet file from local Spark distro.
Ready for spark-submit execution.
"""

from pyspark.sql import SparkSession


def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("ReadUsersParquet") \
        .getOrCreate()
    
    # Read users.parquet file from local filesystem
    # Adjust path as needed for your environment
    df = spark.read.parquet("users.parquet")
    
    # Show schema and sample data
    print("Schema:")
    df.printSchema()
    
    print("\nSample data:")
    df.show(truncate=False)
    
    # Show count
    print(f"\nTotal rows: {df.count()}")
    
    # Stop SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
