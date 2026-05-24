#!/usr/bin/env python3
"""
A simple example demonstrating Arrow in Spark.

Run with:
  ./bin/spark-submit examples/src/main/python/sql/arrow.py

This example demonstrates:
- Using SparkSession with Arrow enabled
- Creating DataFrames
- Converting to pandas with Arrow optimization
- Working with Series, DataFrame, and LongType
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import LongType, StructType, StructField
import pandas as pd


def main():
    # Create SparkSession with Arrow enabled
    spark = SparkSession.builder \
        .appName("ArrowExample") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .getOrCreate()

    print("=" * 60)
    print("Arrow in Spark - Simple Example")
    print("=" * 60)

    # Create a simple DataFrame using Spark SQL
    print("\n1. Creating Spark DataFrame...")
    data = [(i, i * 10, f"value_{i}") for i in range(1, 11)]
    columns = ["id", "value", "label"]
    
    df: DataFrame = spark.createDataFrame(data, columns)
    df.show()
    print(f"DataFrame schema: {df.schema}")
    print(f"Row count: {df.count()}")

    # Demonstrate LongType usage
    print("\n2. Working with LongType...")
    schema_with_long = StructType([
        StructField("id", LongType(), True),
        StructField("value", LongType(), True),
        StructField("label", LongType(), True)
    ])
    
    long_data = [(i, i * 100, i * 1000) for i in range(1, 6)]
    df_long: DataFrame = spark.createDataFrame(long_data, schema_with_long)
    df_long.show()
    print(f"Schema with LongType: {df_long.schema}")

    # Convert Spark DataFrame to pandas using Arrow
    print("\n3. Converting Spark DataFrame to pandas with Arrow...")
    try:
        pandas_df: pd.DataFrame = df.toPandas()
        print(f"Successfully converted to pandas DataFrame!")
        print(f"pandas DataFrame shape: {pandas_df.shape}")
        print(f"pandas DataFrame dtypes:\n{pandas_df.dtypes}")
        print(f"\npandas DataFrame head:\n{pandas_df.head()}")
    except Exception as e:
        print(f"Arrow conversion failed: {e}")
        print("Falling back to standard conversion...")
        pandas_df = df.toPandas()

    # Demonstrate pandas Series operations
    print("\n4. Working with pandas Series...")
    id_series: pd.Series = pandas_df["id"]
    value_series: pd.Series = pandas_df["value"]
    
    print(f"ID Series type: {type(id_series)}")
    print(f"ID Series values: {id_series.tolist()}")
    print(f"Value Series sum: {value_series.sum()}")
    print(f"Value Series mean: {value_series.mean()}")

    # Create pandas DataFrame and convert back to Spark
    print("\n5. Converting pandas DataFrame back to Spark...")
    new_pandas_df = pd.DataFrame({
        "product_id": pd.Series([1, 2, 3, 4, 5], dtype="int64"),
        "quantity": pd.Series([10, 20, 30, 40, 50], dtype="int64"),
        "price": pd.Series([100.5, 200.5, 300.5, 400.5, 500.5], dtype="float64")
    })
    
    new_spark_df: DataFrame = spark.createDataFrame(new_pandas_df)
    new_spark_df.show()
    print(f"New Spark DataFrame schema: {new_spark_df.schema}")

    # SQL query with Arrow optimization
    print("\n6. Running SQL query with Arrow...")
    df.createOrReplaceTempView("arrow_example")
    result_df: DataFrame = spark.sql("""
        SELECT 
            id,
            value,
            label,
            value * 2 as doubled_value
        FROM arrow_example
        WHERE id > 5
    """)
    result_df.show()

    # Show Arrow configuration
    print("\n7. Arrow Configuration:")
    print(f"  spark.sql.execution.arrow.pyspark.enabled: {spark.conf.get('spark.sql.execution.arrow.pyspark.enabled')}")
    print(f"  spark.sql.execution.arrow.pyspark.fallback.enabled: {spark.conf.get('spark.sql.execution.arrow.pyspark.fallback.enabled')}")

    print("\n" + "=" * 60)
    print("Arrow example completed successfully!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
