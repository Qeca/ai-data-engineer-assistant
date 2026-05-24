from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, expr

spark = SparkSession.builder.appName("MySQL Retail Sales Revenue").getOrCreate()

# Read retail_sales from MySQL using JDBC
sales_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://demo-mysql:3306/analytics") \
    .option("dbtable", "retail_sales") \
    .option("user", "demo") \
    .option("password", "demo") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Read retail_products from MySQL using JDBC
products_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://demo-mysql:3306/analytics") \
    .option("dbtable", "retail_products") \
    .option("user", "demo") \
    .option("password", "demo") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Join and calculate revenue
revenue_df = sales_df.join(products_df, "product_id") \
    .agg(spark_sum(expr("quantity * price")).alias("total_revenue"))

result = revenue_df.collect()[0]["total_revenue"]
print(f"Total revenue from MySQL retail_sales: {result}")

spark.stop()