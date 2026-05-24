from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

spark = SparkSession.builder.appName("MySQL Retail Sales").getOrCreate()

# Connect to MySQL retail_sales using JDBC with user in URL
jdbc_url = "jdbc:mysql://demo-mysql:3306/analytics?user=demo&useSSL=false"
df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "retail_sales").load()

total = df.agg(spark_sum("amount")).collect()[0][0]
print(f"RESULT: {total}")

spark.stop()