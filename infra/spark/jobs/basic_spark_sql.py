#!/usr/bin/env python3
"""
A simple example demonstrating basic Spark SQL features.
Run with: ./bin/spark-submit examples/src/main/python/sql/basic.py

Key classes: SparkSession, StructField, StringType, StructType, Row
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType, IntegerType
from pyspark.sql import Row


def main():
    # Create SparkSession - entry point for Spark SQL
    spark = SparkSession.builder \
        .appName("Basic Spark SQL Example") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=" * 60)
    print("Basic Spark SQL Example")
    print("=" * 60)
    
    # 1. Create DataFrame with explicit schema using StructType and StructField
    print("\n1. Creating DataFrame with explicit schema...")
    
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])
    
    data = [
        Row(name="Alice", age=25, city="New York"),
        Row(name="Bob", age=30, city="London"),
        Row(name="Charlie", age=35, city="Paris"),
        Row(name="Diana", age=28, city="Tokyo"),
        Row(name="Eve", age=32, city="Berlin")
    ]
    
    df = spark.createDataFrame(data, schema)
    print(f"DataFrame created with {df.count()} rows")
    df.printSchema()
    
    # 2. Show DataFrame content
    print("\n2. DataFrame content:")
    df.show()
    
    # 3. Basic DataFrame API operations
    print("\n3. DataFrame API operations:")
    
    # Filter
    filtered_df = df.filter(df.age > 30)
    print("Filtered (age > 30):")
    filtered_df.show()
    
    # Select specific columns
    print("Selected columns (name, city):")
    df.select("name", "city").show()
    
    # Add new column
    from pyspark.sql.functions import col, lit
    df_with_country = df.withColumn("country", lit("Unknown"))
    print("DataFrame with new column:")
    df_with_country.show()
    
    # GroupBy and aggregation
    print("Average age by city:")
    df.groupBy("city").avg("age").show()
    
    # 4. Spark SQL - Register DataFrame as temporary view
    print("\n4. Spark SQL queries:")
    df.createOrReplaceTempView("people")
    
    # SQL query
    sql_result = spark.sql("""
        SELECT name, age, city
        FROM people
        WHERE age >= 30
        ORDER BY age DESC
    """)
    print("SQL Query result (age >= 30, ordered by age DESC):")
    sql_result.show()
    
    # 5. Another example with different schema
    print("\n5. Creating products DataFrame...")
    
    product_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", IntegerType(), True)
    ])
    
    products_data = [
        Row(product_id="P001", product_name="Laptop", category="Electronics", price=1200),
        Row(product_id="P002", product_name="Mouse", category="Electronics", price=25),
        Row(product_id="P003", product_name="Desk", category="Furniture", price=300),
        Row(product_id="P004", product_name="Chair", category="Furniture", price=150),
        Row(product_id="P005", product_name="Notebook", category="Stationery", price=5)
    ]
    
    products_df = spark.createDataFrame(products_data, product_schema)
    products_df.createOrReplaceTempView("products")
    
    print("Products DataFrame:")
    products_df.show()
    
    # SQL join example
    print("\n6. SQL Join example (if we had related data)...")
    # For demonstration, just show products by category
    spark.sql("""
        SELECT category, COUNT(*) as product_count, AVG(price) as avg_price
        FROM products
        GROUP BY category
        ORDER BY product_count DESC
    """).show()
    
    # 7. Collect results to driver
    print("\n7. Collecting results to driver:")
    all_people = df.collect()
    for person in all_people:
        print(f"  - {person.name}: {person.age} years old, lives in {person.city}")
    
    print("\n" + "=" * 60)
    print("Spark SQL example completed successfully!")
    print("=" * 60)
    
    # Stop SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
