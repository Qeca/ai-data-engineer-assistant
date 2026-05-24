#!/usr/bin/env python3
"""
Structured Streaming Network WordCount with Session Window.

Split lines into words, group by words and use the state per key to track session of each key.
Each session window sets a 10 seconds processing time timeout.
After 10 seconds of idle period, the session summary will be finalized and output to sink.

Usage: structured_network_wordcount_windowed.py <hostname> <port>
<hostname> and <port> describe the TCP server that Structured Streaming would connect to receive data.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, session_window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType


def main():
    if len(sys.argv) < 3:
        print("Usage: structured_network_wordcount_windowed.py <hostname> <port>", file=sys.stderr)
        sys.exit(1)

    hostname = sys.argv[1]
    port = int(sys.argv[2])

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("StructuredNetworkWordCountWindowed") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define schema for incoming data
    # Each line from socket is a string
    lines_schema = StructType([
        StructField("value", StringType(), True)
    ])

    # Create DataFrame representing the stream of input lines from connection to hostname:port
    lines = spark.readStream \
        .format("socket") \
        .option("host", hostname) \
        .option("port", port) \
        .schema(lines_schema) \
        .load()

    # Split lines into words
    # explode splits each line into multiple rows (one per word)
    words = lines.select(
        explode(split(col("value"), " ")).alias("word"),
        col("value").alias("line")
    )

    # Add a processing time timestamp column for session window
    # session_window requires a timestamp column
    from pyspark.sql.functions import current_timestamp
    words_with_time = words.withColumn("processing_time", current_timestamp())

    # Group by word and apply session window with 10 seconds timeout
    # session_window creates sessions based on gaps between events
    # gapDuration=10 seconds means session ends after 10 seconds of inactivity
    word_counts = words_with_time.groupBy(
        col("word"),
        session_window(col("processing_time"), "10 seconds").alias("session")
    ).count()

    # Start the query and output to console
    # outputMode("complete") - output all rows in the result table
    # outputMode("append") - only new rows (for session windows, use "update" or "complete")
    query = word_counts.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()

    # Wait for termination
    query.awaitTermination()


if __name__ == "__main__":
    main()
