from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

spark = SparkSession.builder.appName("MySQL Retail Sales").getOrCreate()

# Connect to MySQL retail_sales
df = spark.read.format("jdbc").option("url", "jdbc:mysql://demo-mysql:3306/analytics?user=demo").option("dbtable", "retail_sales").load()

total = df.agg(spark_sum("amount")).collect()[0][0]
print(f"MySQL retail_sales total revenue: {total}")

spark.stop()