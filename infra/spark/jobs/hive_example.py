#!/usr/bin/env python3
"""
Simple example demonstrating Spark SQL Hive integration.
Run with: ./bin/spark-submit examples/src/main/python/sql/hive.py

This script demonstrates:
- SparkSession with Hive support
- DataFrame API usage
- Row and Record handling
- COUNT and other aggregations
- Hive table operations
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import count, sum, avg, col, lit


def create_spark_session():
    """Create SparkSession with Hive support enabled."""
    spark = SparkSession.builder \
        .appName("HiveIntegrationExample") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_sample_data(spark):
    """Create sample data using DataFrame API and Row."""
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True),
        StructField("department", StringType(), True)
    ])
    
    # Create data using Row
    employees = [
        Row(id=1, name="Alice", age=30, salary=50000.0, department="Engineering"),
        Row(id=2, name="Bob", age=25, salary=45000.0, department="Engineering"),
        Row(id=3, name="Charlie", age=35, salary=60000.0, department="Sales"),
        Row(id=4, name="Diana", age=28, salary=55000.0, department="Sales"),
        Row(id=5, name="Eve", age=32, salary=70000.0, department="Engineering"),
        Row(id=6, name="Frank", age=40, salary=80000.0, department="Management"),
        Row(id=7, name="Grace", age=29, salary=52000.0, department="Engineering"),
        Row(id=8, name="Henry", age=38, salary=65000.0, department="Sales"),
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(employees, schema)
    return df


def create_hive_table(spark, df, table_name):
    """Create a Hive managed table from DataFrame."""
    # Drop table if exists
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    # Save as Hive table
    df.write.saveAsTable(table_name)
    print(f"✓ Created Hive table: {table_name}")


def demonstrate_dataframe_api(spark, df):
    """Demonstrate DataFrame API operations."""
    print("\n" + "="*60)
    print("DataFrame API Operations")
    print("="*60)
    
    # Show schema
    print("\nSchema:")
    df.printSchema()
    
    # Show data
    print("\nSample data:")
    df.show(10)
    
    # Filter operations
    print("\nEmployees in Engineering department:")
    df.filter(col("department") == "Engineering").show()
    
    # Aggregations with COUNT
    print("\nCOUNT operations:")
    
    # Total count
    total_count = df.count()
    print(f"Total employees: {total_count}")
    
    # Count by department
    print("\nCount by department:")
    df.groupBy("department").agg(
        count("*").alias("employee_count"),
        avg("salary").alias("avg_salary"),
        sum("salary").alias("total_salary")
    ).show()
    
    # Count distinct
    distinct_depts = df.select("department").distinct().count()
    print(f"\nDistinct departments: {distinct_depts}")


def demonstrate_spark_sql(spark, table_name):
    """Demonstrate Spark SQL with Hive tables."""
    print("\n" + "="*60)
    print("Spark SQL Operations")
    print("="*60)
    
    # Register temporary view
    spark.sql(f"SELECT * FROM {table_name}").createOrReplaceTempView("employees_view")
    
    # Basic SELECT with COUNT
    print("\nBasic COUNT query:")
    result = spark.sql(f"""
        SELECT 
            department,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary,
            MIN(salary) as min_salary,
            MAX(salary) as max_salary
        FROM {table_name}
        GROUP BY department
        ORDER BY employee_count DESC
    """)
    result.show()
    
    # Complex aggregation
    print("\nComplex aggregation with HAVING:")
    result = spark.sql(f"""
        SELECT 
            department,
            COUNT(*) as cnt,
            AVG(age) as avg_age,
            SUM(salary) as total_payroll
        FROM {table_name}
        GROUP BY department
        HAVING COUNT(*) > 1
        ORDER BY total_payroll DESC
    """)
    result.show()
    
    # Join example (self-join for demonstration)
    print("\nSelf-join example (employees with same department):")
    spark.sql(f"""
        SELECT 
            e1.name as employee1,
            e2.name as employee2,
            e1.department
        FROM {table_name} e1
        JOIN {table_name} e2 
            ON e1.department = e2.department 
            AND e1.id < e2.id
        LIMIT 5
    """).show()


def demonstrate_record_handling(spark):
    """Demonstrate Record/Row handling."""
    print("\n" + "="*60)
    print("Record/Row Handling")
    print("="*60)
    
    # Create records using Row
    records = [
        Row(product_id=101, product_name="Laptop", price=999.99, quantity=5),
        Row(product_id=102, product_name="Mouse", price=29.99, quantity=50),
        Row(product_id=103, product_name="Keyboard", price=79.99, quantity=30),
        Row(product_id=104, product_name="Monitor", price=299.99, quantity=10),
    ]
    
    df = spark.createDataFrame(records)
    
    # Access Row fields
    print("\nAccessing Row fields:")
    first_row = df.first()
    print(f"First product: {first_row['product_name']}")
    print(f"Price: ${first_row['price']}")
    print(f"Quantity in stock: {first_row['quantity']}")
    
    # Convert to list of tuples
    print("\nAs tuples:")
    for row in df.collect():
        print(f"  {tuple(row)}")
    
    # Map operation
    print("\nMapped data (product with total value):")
    df.withColumn("total_value", col("price") * col("quantity")).show()


def list_hive_tables(spark):
    """List all Hive tables."""
    print("\n" + "="*60)
    print("Hive Tables")
    print("="*60)
    
    tables = spark.sql("SHOW TABLES").collect()
    if tables:
        print("\nAvailable Hive tables:")
        for table in tables:
            print(f"  - {table['tableName']}")
    else:
        print("\nNo Hive tables found.")


def main():
    """Main function demonstrating Hive integration."""
    print("="*60)
    print("Spark SQL Hive Integration Example")
    print("="*60)
    
    # Create SparkSession with Hive support
    spark = create_spark_session()
    
    try:
        # Create sample data
        print("\nCreating sample employee data...")
        employees_df = create_sample_data(spark)
        
        # Create Hive table
        table_name = "employees_hive"
        create_hive_table(spark, employees_df, table_name)
        
        # Demonstrate DataFrame API
        demonstrate_dataframe_api(spark, employees_df)
        
        # Demonstrate Spark SQL
        demonstrate_spark_sql(spark, table_name)
        
        # Demonstrate Record handling
        demonstrate_record_handling(spark)
        
        # List Hive tables
        list_hive_tables(spark)
        
        print("\n" + "="*60)
        print("Example completed successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"\nError: {e}")
        raise
    
    finally:
        spark.stop()
        print("\nSparkSession stopped.")


if __name__ == "__main__":
    main()
