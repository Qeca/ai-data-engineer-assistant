#!/usr/bin/env python3
"""
Structured Network Word Count using PySpark Structured Streaming.

Counts words in UTF8 encoded, '\n' delimited text received from the network.

Usage: structured_network_wordcount.py <hostname> <port>
  <hostname> and <port> describe the TCP server that Structured Streaming
  would connect to receive data.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col


def main():
    if len(sys.argv) != 3:
        print("Usage: structured_network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(1)

    hostname = sys.argv[1]
    port = int(sys.argv[2])

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Create DataFrame representing the stream of input lines from connection to localhost:port
    lines = spark.readStream \
        .format("socket") \
        .option("host", hostname) \
        .option("port", port) \
        .load()

    # Split the lines into words
    words = lines.select(
        explode(split(col("value"), " ")).alias("word")
    )

    # Generate running word count
    wordCounts = words.groupBy("word").count()

    # Start running the query that prints the running counts to the console
    query = wordCounts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
