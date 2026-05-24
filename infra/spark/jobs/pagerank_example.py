"""
PageRank Example Implementation in PySpark

This is an example implementation of PageRank. For more conventional use,
please refer to PageRank implementation provided by GraphX.

Usage:
    spark-submit pagerank_example.py [input_file] [num_iterations]

Example:
    spark-submit pagerank_example.py links.txt 10
"""

import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


def compute_contributions(rank, num_neighbors):
    """Compute contribution from a page to its neighbors."""
    return rank / num_neighbors


def parse_links(line):
    """Parse a line of the form 'URL1 URL2' into (URL1, URL2)."""
    parts = line.split()
    if len(parts) >= 2:
        return (parts[0], parts[1])
    return None


def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PageRank Example") \
        .getOrCreate()
    
    sc = spark.sparkContext
    
    # Default parameters
    input_file = "links.txt"
    num_iterations = 10
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        num_iterations = int(sys.argv[2])
    
    print(f"Running PageRank on {input_file} for {num_iterations} iterations")
    
    # Load and parse the input data
    # Each line is of the form: "URL1 URL2" meaning URL1 links to URL2
    try:
        lines = sc.textFile(input_file)
    except Exception:
        # If file doesn't exist, create sample data for demonstration
        print("Input file not found, using sample data for demonstration")
        sample_links = [
            "A B",
            "A C",
            "B C",
            "C A",
            "D A",
            "D B"
        ]
        lines = sc.parallelize(sample_links)
    
    # Parse links into (source, destination) pairs
    links = lines.map(lambda line: parse_links(line)) \
        .filter(lambda x: x is not None) \
        .distinct() \
        .groupByKey() \
        .cache()
    
    # Initialize ranks to 1.0 for each URL
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    
    # Calculate out-degree for each URL
    out_degrees = links.mapValues(lambda neighbors: len(list(neighbors)))
    
    print(f"Number of unique URLs: {ranks.count()}")
    print(f"Number of links: {links.count()}")
    
    # PageRank iterations
    for iteration in range(num_iterations):
        # Join ranks with out-degrees and compute contributions
        # (url, (rank, out_degree))
        ranks_with_degree = ranks.join(out_degrees)
        
        # Compute contributions: (neighbor_url, contribution)
        contributions = ranks_with_degree.flatMap(
            lambda url_rank_degree: [
                (neighbor, compute_contributions(url_rank_degree[1][0], url_rank_degree[1][1]))
                for neighbor in links.lookup(url_rank_degree[0])[0]
            ]
        )
        
        # Sum contributions and apply damping factor
        # New rank = (1 - damping_factor) + damping_factor * sum(contributions)
        damping_factor = 0.85
        ranks = contributions.reduceByKey(lambda a, b: a + b) \
            .mapValues(lambda rank_sum: (1 - damping_factor) + damping_factor * rank_sum)
        
        # Print progress every 5 iterations
        if (iteration + 1) % 5 == 0 or iteration == 0:
            top_ranks = ranks.takeOrdered(10, key=lambda x: -x[1])
            print(f"\nIteration {iteration + 1}:")
            for url, rank in top_ranks:
                print(f"  {url}: {rank:.6f}")
    
    # Collect and display final results
    print("\n" + "="*50)
    print("Final PageRank Results:")
    print("="*50)
    
    final_ranks = ranks.collect()
    final_ranks_sorted = sorted(final_ranks, key=lambda x: -x[1])
    
    for url, rank in final_ranks_sorted:
        print(f"{url}: {rank:.6f}")
    
    # Save results to output file
    try:
        ranks.saveAsTextFile("pagerank_output")
        print("\nResults saved to pagerank_output/")
    except Exception as e:
        print(f"\nCould not save results: {e}")
    
    spark.stop()
    print("\nPageRank completed successfully!")


if __name__ == "__main__":
    main()
