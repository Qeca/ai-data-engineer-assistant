from pyspark.sql import SparkSession

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("ReadUsersAvro") \
        .getOrCreate()
    
    # Read Avro file from local Spark distro
    df = spark.read.format("avro").load("users.avro")
    
    # Show schema and data
    df.printSchema()
    df.show(truncate=False)
    
    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
