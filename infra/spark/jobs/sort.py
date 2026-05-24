#!/usr/bin/env python3
"""
PySpark script for sorting data.
Ready for spark-submit execution.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("SortJob") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Example: Create sample DataFrame
    data = [
        (1, "Alice", 30),
        (3, "Charlie", 25),
        (2, "Bob", 35),
        (5, "Eve", 28),
        (4, "David", 32)
    ]
    columns = ["id", "name", "age"]
    
    df = spark.createDataFrame(data, columns)
    
    print("Original DataFrame:")
    df.show()
    
    # Sort by id ascending
    df_sorted = df.orderBy(col("id").asc())
    
    print("Sorted by id (ascending):")
    df_sorted.show()
    
    # Sort by age descending
    df_sorted_age = df.orderBy(col("age").desc())
    
    print("Sorted by age (descending):")
    df_sorted_age.show()
    
    # Stop SparkSession
    spark.stop()
    
    print("Job completed successfully!")


if __name__ == "__main__":
    main()
