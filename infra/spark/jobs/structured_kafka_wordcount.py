#!/usr/bin/env python3
"""
Structured Kafka WordCount example.
Consumes messages from one or more topics in Kafka and does wordcount.

Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
  <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A comma-separated list of host:port.
  <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe', 'subscribePattern'.
    - assign: Specific TopicPartitions to consume. Json string {"topicA":[0,1],"topicB":[2,4]}.
    - subscribe: The topic list to subscribe. A comma-separated list of topics.
    - subscribePattern: The pattern to subscribe. A regex pattern string.
  <topics> The topic list or pattern, depending on subscribe-type.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count
from pyspark.sql.types import StringType


def main():
    if len(sys.argv) < 4:
        print("Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>", file=sys.stderr)
        print("  <bootstrap-servers> The Kafka 'bootstrap.servers' configuration. A comma-separated list of host:port.", file=sys.stderr)
        print("  <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe', 'subscribePattern'.", file=sys.stderr)
        print("    - assign: Specific TopicPartitions to consume. Json string {\"topicA\":[0,1],\"topicB\":[2,4]}.", file=sys.stderr)
        print("    - subscribe: The topic list to subscribe. A comma-separated list of topics.", file=sys.stderr)
        print("    - subscribePattern: The pattern to subscribe. A regex pattern string.", file=sys.stderr)
        print("  <topics> The topic list or pattern, depending on subscribe-type.", file=sys.stderr)
        sys.exit(1)

    bootstrap_servers = sys.argv[1]
    subscribe_type = sys.argv[2]
    topics = sys.argv[3]

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("StructuredKafkaWordCount") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Create DataFrame representing the stream of input lines from Kafka
    if subscribe_type == "assign":
        # assign expects a JSON string with topic partitions
        lines = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("assign", topics) \
            .load()
    elif subscribe_type == "subscribe":
        # subscribe expects a comma-separated list of topics
        lines = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topics) \
            .load()
    elif subscribe_type == "subscribePattern":
        # subscribePattern expects a regex pattern
        lines = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribePattern", topics) \
            .load()
    else:
        print(f"Error: Unknown subscribe-type '{subscribe_type}'. Use 'assign', 'subscribe', or 'subscribePattern'.", file=sys.stderr)
        sys.exit(1)

    # Convert Kafka value (binary) to string
    words = lines.select(
        col("value").cast(StringType()).alias("line")
    ).select(
        explode(split(col("line"), " ")).alias("word")
    )

    # Generate running word count
    word_counts = words.groupBy("word").agg(
        count("word").alias("count")
    )

    # Start running the query that prints the running counts to the console
    query = word_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
