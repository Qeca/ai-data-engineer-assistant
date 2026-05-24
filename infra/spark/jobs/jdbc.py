#!/usr/bin/env python3
"""
Simple example demonstrating Spark SQL JDBC integration.

Run with:
  ./bin/spark-submit examples/src/main/python/sql/jdbc.py [jdbc_url]

Uses SparkSession and DataFrame API for reading and writing via JDBC.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum


def main():
    # Get JDBC URL from command line arguments or use default
    if len(sys.argv) > 1:
        jdbc_url = sys.argv[1]
    else:
        # Default PostgreSQL JDBC URL for demo
        jdbc_url = "jdbc:postgresql://localhost:5432/mydb"
    
    print(f"Using JDBC URL: {jdbc_url}")
    
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("JDBC Example") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    
    # Set logging level
    spark.sparkContext.setLogLevel("WARN")
    
    print("SparkSession created successfully")
    print(f"Spark version: {spark.version}")
    
    # JDBC connection properties
    # Note: In production, use Spark secrets or external credential management
    # Pass credentials via spark-submit --conf options or secrets manager
    properties = {
        "user": "db_user",
        "driver": "org.postgresql.Driver"
    }
    
    # Example 1: Read data from database via JDBC
    print("\n=== Reading data via JDBC ===")
    
    try:
        # Read entire table
        df = spark.read.jdbc(
            url=jdbc_url,
            table="my_table",
            properties=properties
        )
        
        print(f"Schema:")
        df.printSchema()
        
        print(f"\nRow count: {df.count()}")
        
        print(f"\nFirst 5 rows:")
        df.show(5, truncate=False)
        
        # Example 2: DataFrame transformations
        print("\n=== DataFrame Transformations ===")
        
        # Filter and aggregate
        if "amount" in df.columns and "status" in df.columns:
            result = df.filter(col("status") == "active") \
                .groupBy("status") \
                .agg(
                    count("*").alias("record_count"),
                    spark_sum("amount").alias("total_amount")
                )
            
            print("Active records summary:")
            result.show()
        
        # Example 3: Write data back via JDBC
        print("\n=== Writing data via JDBC ===")
        
        # Create a simple DataFrame to write
        sample_data = [
            (1, "test_record", 100.0, "active"),
            (2, "test_record_2", 200.0, "inactive")
        ]
        sample_df = spark.createDataFrame(
            sample_data,
            ["id", "name", "amount", "status"]
        )
        
        print("Sample data to write:")
        sample_df.show()
        
        # Write to database (mode: append, overwrite, ignore, errorifexists)
        # Uncomment to actually write:
        # sample_df.write.jdbc(
        #     url=jdbc_url,
        #     table="my_output_table",
        #     mode="overwrite",
        #     properties=properties
        # )
        # print("Data written successfully!")
        
        print("\n=== JDBC Example Completed ===")
        
    except Exception as e:
        print(f"Error during JDBC operations: {e}")
        import traceback
        traceback.print_exc()
    
    # Stop SparkSession
    spark.stop()
    print("SparkSession stopped")


if __name__ == "__main__":
    main()
