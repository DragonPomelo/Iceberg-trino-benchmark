import logging
import time

import matplotlib.pyplot as plt
import numpy as np
import trino

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_to_trino():
    conn = trino.dbapi.connect(
        host="localhost",
        port=8080,
        user="admin",
        catalog="example",
        schema="benchmark",
    )
    return conn


def execute_query(conn, query):
    cursor = conn.cursor()
    try:
        start_time = time.time()
        cursor.execute(query)
        rows = cursor.fetchall()
        end_time = time.time()
        return end_time - start_time, rows
    except trino.exceptions.TrinoQueryError as e:
        logger.error(f"Query failed: {e}")
        raise


def create_schema(conn):
    query = """
    CREATE SCHEMA IF NOT EXISTS example.benchmark 
    WITH (location = 's3a://warehouse/benchmark')
    """
    execute_query(conn, query)


def create_tables(conn):
    table_creation_times = {}
    table_row_counts = {}
    try:
        # Create table without Bloom Filter
        query = """
        CREATE or replace TABLE table_without_bloom_filter 
        WITH (
            format = 'PARQUET'
        ) AS
        SELECT * FROM tpch.sf30.customer
        """
        table_creation_times["table_without_bloom_filter"], _ = execute_query(
            conn, query
        )
        query = "SELECT COUNT(*) FROM table_without_bloom_filter"
        _, rows = execute_query(conn, query)
        table_row_counts["table_without_bloom_filter"] = rows[0][0]

        # Create table with Bloom Filter
        query = """
        CREATE or replace TABLE table_with_bloom_filter 
        WITH (
            format = 'PARQUET',
            parquet_bloom_filter_columns = ARRAY['custkey']
        )
        AS
        SELECT * FROM tpch.sf30.customer
        """
        table_creation_times["table_with_bloom_filter"], _ = execute_query(conn, query)
        query = "SELECT COUNT(*) FROM table_with_bloom_filter"
        _, rows = execute_query(conn, query)
        table_row_counts["table_with_bloom_filter"] = rows[0][0]

        # Create table with sorting
        query = """
        CREATE or replace TABLE table_with_sorting
        WITH (
            format = 'PARQUET',
            sorted_by = ARRAY['custkey']
        )
        AS
        SELECT * FROM tpch.sf30.customer
        """
        table_creation_times["table_with_sorting"], _ = execute_query(conn, query)
        query = "SELECT COUNT(*) FROM table_with_sorting"
        _, rows = execute_query(conn, query)
        table_row_counts["table_with_sorting"] = rows[0][0]

        # Create table with both sorting and Bloom Filter
        query = """
        CREATE or replace TABLE table_with_sorting_and_bloom_filter
        WITH (
            format = 'PARQUET',
            sorted_by = ARRAY['custkey'],
            parquet_bloom_filter_columns = ARRAY['custkey']
        )
        AS
        SELECT * FROM tpch.sf30.customer
        """
        table_creation_times["table_with_sorting_and_bloom_filter"], _ = execute_query(
            conn, query
        )
        query = "SELECT COUNT(*) FROM table_with_sorting_and_bloom_filter"
        _, rows = execute_query(conn, query)
        table_row_counts["table_with_sorting_and_bloom_filter"] = rows[0][0]

    except trino.exceptions.TrinoQueryError as e:
        logger.error(f"Failed to create tables: {e}")
        raise

    return table_creation_times, table_row_counts


def benchmark_queries(conn):
    queries = [
        "SELECT * FROM {} WHERE custkey = 500000",
        "SELECT COUNT(*) FROM {} WHERE custkey > 500000",
    ]

    table_names = [
        "table_without_bloom_filter",
        "table_with_bloom_filter",
        "table_with_sorting",
        "table_with_sorting_and_bloom_filter",
    ]

    results = {table: [] for table in table_names}

    for table in table_names:
        for query in queries:
            formatted_query = query.format(table)
            try:
                execution_time, _ = execute_query(conn, formatted_query)
                results[table].append(execution_time)
            except trino.exceptions.TrinoQueryError as e:
                logger.error(f"Failed to execute query on {table}: {e}")

    return results


def plot_results(average_query_results, average_creation_times):
    queries = [
        "SELECT * FROM {} WHERE custkey = 500000",
        "SELECT COUNT(*) FROM {} WHERE custkey > 500000",
    ]

    table_names = list(average_query_results.keys())
    query_labels = [f"Query {i+1}" for i in range(len(queries))]

    for i, query in enumerate(queries):
        execution_times = [average_query_results[table][i] for table in table_names]
        plt.figure(figsize=(10, 6))
        plt.bar(table_names, execution_times, color="skyblue")
        plt.xlabel("Table Name")
        plt.ylabel("Execution Time (seconds)")
        plt.title(f"Average Execution Time for {query}")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    creation_times_list = [average_creation_times[table] for table in table_names]
    plt.figure(figsize=(10, 6))
    plt.bar(table_names, creation_times_list, color="lightgreen")
    plt.xlabel("Table Name")
    plt.ylabel("Creation Time (seconds)")
    plt.title("Average Table Creation Times")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def main():
    conn = connect_to_trino()

    # Create schema
    create_schema(conn)

    num_runs = 3
    creation_times_list = []
    query_results_list = []
    row_counts = None

    for _ in range(num_runs):
        # Create tables and measure creation times and row counts
        creation_times, row_counts = create_tables(conn)
        creation_times_list.append(creation_times)

        # Benchmark queries
        results = benchmark_queries(conn)
        query_results_list.append(results)

    # Calculate average creation times
    average_creation_times = {
        table: np.mean([run[table] for run in creation_times_list])
        for table in creation_times_list[0].keys()
    }

    # Calculate average query execution times
    average_query_results = {
        table: [
            np.mean([run[table][i] for run in query_results_list])
            for i in range(len(query_results_list[0][table]))
        ]
        for table in query_results_list[0].keys()
    }

    # Print average results
    for table, times in average_query_results.items():
        print(f"Average performance for {table}:")
        for i, time in enumerate(times):
            print(f"Query {i+1}: {time} seconds")

    # Print average creation times
    for table, creation_time in average_creation_times.items():
        print(f"Average creation time for {table}: {creation_time} seconds")

    # Print row counts
    for table, row_count in row_counts.items():
        print(f"Row count for {table}: {row_count}")

    # Plot average results
    plot_results(average_query_results, average_creation_times)


if __name__ == "__main__":
    main()
