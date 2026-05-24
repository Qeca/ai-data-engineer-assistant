#!/usr/bin/env python3
"""
A simple example demonstrating Spark SQL data sources.
Run with: ./bin/spark-submit examples/src/main/python/sql/datasource.py

Uses SparkSession and DataFrame API with key classes: CHAR, VARCHAR, DECIMAL, Row.
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DoubleType, DateType, TimestampType
)
from decimal import Decimal
from datetime import date, datetime


def create_spark_session():
    """Create SparkSession with SQL support."""
    spark = SparkSession.builder \
        .appName("SparkSQLDataSourceExample") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def demonstrate_basic_types(spark):
    """Demonstrate CHAR, VARCHAR, DECIMAL and other types."""
    print("=" * 60)
    print("1. Demonstrating Basic Data Types (CHAR, VARCHAR, DECIMAL)")
    print("=" * 60)
    
    # Create sample data with various types
    data = [
        Row(
            id=1,
            char_code="A01",           # CHAR(3)
            varchar_name="John Doe",   # VARCHAR(50)
            price=Decimal("99.99"),    # DECIMAL(10,2)
            quantity=5,
            discount=Decimal("0.15"),  # DECIMAL(5,4)
            category="Electronics",
            created_date=date(2024, 1, 15),
            updated_ts=datetime(2024, 1, 15, 10, 30, 0)
        ),
        Row(
            id=2,
            char_code="B02",
            varchar_name="Jane Smith",
            price=Decimal("149.50"),
            quantity=3,
            discount=Decimal("0.10"),
            category="Books",
            created_date=date(2024, 1, 16),
            updated_ts=datetime(2024, 1, 16, 14, 45, 0)
        ),
        Row(
            id=3,
            char_code="C03",
            varchar_name="Bob Johnson",
            price=Decimal("29.99"),
            quantity=10,
            discount=Decimal("0.20"),
            category="Home",
            created_date=date(2024, 1, 17),
            updated_ts=datetime(2024, 1, 17, 9, 15, 0)
        ),
    ]
    
    # Define schema explicitly
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("char_code", StringType(), True),      # CHAR(3)
        StructField("varchar_name", StringType(), True),   # VARCHAR(50)
        StructField("price", DecimalType(10, 2), True),    # DECIMAL(10,2)
        StructField("quantity", IntegerType(), True),
        StructField("discount", DecimalType(5, 4), True),  # DECIMAL(5,4)
        StructField("category", StringType(), True),
        StructField("created_date", DateType(), True),
        StructField("updated_ts", TimestampType(), True),
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    
    print("\nDataFrame Schema:")
    df.printSchema()
    
    print("\nDataFrame Content:")
    df.show(truncate=False)
    
    # SQL query with type casting
    df.createOrReplaceTempView("products")
    
    result = spark.sql("""
        SELECT 
            id,
            char_code,
            varchar_name,
            CAST(price AS DECIMAL(10,2)) as price,
            quantity,
            CAST(discount AS DECIMAL(5,4)) as discount,
            category,
            CAST(price * (1 - discount) AS DECIMAL(10,2)) as final_price,
            created_date
        FROM products
        ORDER BY id
    """)
    
    print("\nSQL Query Result (with calculated final_price):")
    result.show(truncate=False)
    
    return df


def demonstrate_csv_source(spark, df):
    """Demonstrate CSV data source."""
    print("\n" + "=" * 60)
    print("2. Demonstrating CSV Data Source")
    print("=" * 60)
    
    # Write to CSV
    csv_path = "/tmp/products.csv"
    df.write.mode("overwrite").option("header", "true").csv(csv_path)
    print(f"\nWritten DataFrame to CSV: {csv_path}")
    
    # Read from CSV
    df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    print("\nRead from CSV:")
    df_csv.printSchema()
    df_csv.show(truncate=False)
    
    return df_csv


def demonstrate_json_source(spark, df):
    """Demonstrate JSON data source."""
    print("\n" + "=" * 60)
    print("3. Demonstrating JSON Data Source")
    print("=" * 60)
    
    # Write to JSON
    json_path = "/tmp/products.json"
    df.write.mode("overwrite").json(json_path)
    print(f"\nWritten DataFrame to JSON: {json_path}")
    
    # Read from JSON
    df_json = spark.read.json(json_path)
    print("\nRead from JSON:")
    df_json.printSchema()
    df_json.show(truncate=False)
    
    return df_json


def demonstrate_parquet_source(spark, df):
    """Demonstrate Parquet data source."""
    print("\n" + "=" * 60)
    print("4. Demonstrating Parquet Data Source")
    print("=" * 60)
    
    # Write to Parquet
    parquet_path = "/tmp/products.parquet"
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"\nWritten DataFrame to Parquet: {parquet_path}")
    
    # Read from Parquet
    df_parquet = spark.read.parquet(parquet_path)
    print("\nRead from Parquet:")
    df_parquet.printSchema()
    df_parquet.show(truncate=False)
    
    return df_parquet


def demonstrate_dataframe_api(spark, df):
    """Demonstrate DataFrame API operations."""
    print("\n" + "=" * 60)
    print("5. Demonstrating DataFrame API Operations")
    print("=" * 60)
    
    from pyspark.sql import functions as F
    
    # Filter
    print("\nFilter: quantity > 5")
    df.filter(F.col("quantity") > 5).show()
    
    # Select with expressions
    print("\nSelect with expressions:")
    df.select(
        F.col("varchar_name"),
        F.col("price"),
        F.col("discount"),
        (F.col("price") * F.col("quantity")).alias("total_value"),
        (F.col("price") * (1 - F.col("discount"))).alias("discounted_price")
    ).show()
    
    # GroupBy aggregation
    print("\nGroupBy category with aggregations:")
    df.groupBy("category").agg(
        F.count("*").alias("product_count"),
        F.sum("quantity").alias("total_quantity"),
        F.avg("price").alias("avg_price"),
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price")
    ).show()
    
    # OrderBy
    print("\nOrderBy price descending:")
    df.orderBy(F.col("price").desc()).show()
    
    # WithColumn (add new column)
    print("\nWithColumn - add price_category:")
    df.withColumn(
        "price_category",
        F.when(F.col("price") > 100, "Premium")
         .when(F.col("price") > 50, "Mid-range")
         .otherwise("Budget")
    ).show(truncate=False)


def demonstrate_sql_queries(spark, df):
    """Demonstrate SQL queries on DataFrame."""
    print("\n" + "=" * 60)
    print("6. Demonstrating SQL Queries")
    print("=" * 60)
    
    df.createOrReplaceTempView("products_view")
    
    # Basic SELECT
    print("\nBasic SELECT:")
    spark.sql("SELECT * FROM products_view").show()
    
    # WHERE clause
    print("\nWHERE clause (price > 50):")
    spark.sql("SELECT varchar_name, price, category FROM products_view WHERE price > 50").show()
    
    # JOIN example (self-join for demonstration)
    print("\nSelf-JOIN example:")
    spark.sql("""
        SELECT 
            a.varchar_name as customer1,
            b.varchar_name as customer2,
            a.category as cat1,
            b.category as cat2
        FROM products_view a
        JOIN products_view b ON a.category = b.category
        WHERE a.id < b.id
    """).show()
    
    # Subquery
    print("\nSubquery (products above average price):")
    spark.sql("""
        SELECT varchar_name, price, category
        FROM products_view
        WHERE price > (SELECT AVG(price) FROM products_view)
    """).show()


def main():
    """Main function to run all demonstrations."""
    print("\n" + "#" * 60)
    print("# Spark SQL Data Sources Example")
    print("# Demonstrating: CHAR, VARCHAR, DECIMAL, Row, DataFrame API")
    print("#" * 60 + "\n")
    
    # Create SparkSession
    spark = create_spark_session()
    
    try:
        # 1. Basic types demonstration
        df = demonstrate_basic_types(spark)
        
        # 2. CSV source
        demonstrate_csv_source(spark, df)
        
        # 3. JSON source
        demonstrate_json_source(spark, df)
        
        # 4. Parquet source
        demonstrate_parquet_source(spark, df)
        
        # 5. DataFrame API
        demonstrate_dataframe_api(spark, df)
        
        # 6. SQL queries
        demonstrate_sql_queries(spark, df)
        
        print("\n" + "#" * 60)
        print("# All demonstrations completed successfully!")
        print("#" * 60 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
