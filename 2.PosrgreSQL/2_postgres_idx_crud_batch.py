"""
PostgreSQL CRUD Operations and Performance Benchmarking Script 

This script is designed to perform various CRUD (Create, Read, Update, Delete) operations on a PostgreSQL database
and to measure the performance of these operations on large datasets in batches. It automates the following steps:

1. Database and Table Creation: 
   - Dynamically creates a database based on file information and iterates through multiple CRUD operations.
   - Creates tables for nodes, edges, and labels, each with specified indices for optimized query performance.

2. Data Loading:
   - Loads node, edge, and label data from CSV files into the database.
   - Measures and logs the performance (time and space) of data uploads.

3. Read and Indexing Performance:
   - Performs read operations with configurable hops and measures execution times.
   - Logs performance metrics for both read operations and index creation.

4. Update Operations:
   - Updates node and edge records, assigning random weights to a percentage of records and measuring execution time.
   
5. Delete Operations:
   - Deletes all records from each table, resetting the database for the next iteration.
   - Measures the performance of the delete operation.

6. Performance Logging:
   - Logs the results of each operation, including mean execution time, standard deviation, and space used, 
     to an external log file for further analysis.

This script supports custom configurations, such as:
- File paths for data (node, edge, label CSV files)
- Number of iterations for each operation
- Number of hops for read operations

Usage:
Before running, ensure PostgreSQL is running and accessible with the specified credentials. Execute the script from the command line 
or an IDE, providing any necessary file paths and configurations within the main block.

Dependencies:
- 'psycopg2': For connecting to PostgreSQL.
- 'numpy': For numerical operations.
- 'random': For generating random numbers.
- 'time': For performance timing.

"""

# Import required libraries
import psycopg2
import numpy as np
import csv
import re
import time
import random
from psycopg2 import sql

# Specify the path where the log file will be saved
log_file_path = "E:/Master Thesis/GitHub_Repo/Master-Thesis/Code_Final/logs/postgres_crud_operations_log_10iterations.txt"

# log function to store results for all CRUD operations
def log_results(dataset_name, operation_name, mean_exec_time, std_dev_exec_time, mean_size, std_dev_size, log_file_path):
    """
    Logs the performance results of a database operation to a log file.

    Parameters:
        database_name (str): Name of the database.
        operation (str): Name of the operation being logged.
        mean_exec_time (float): Mean execution time of the operation.
        std_dev_exec_time (float): Standard deviation of the execution time.
        mean_space (float): Mean space usage of the operation.
        std_dev_space (float): Standard deviation of the space usage.
        log_file_path (str): Path to the log file.

    Returns:
        None
    """

    # Open the log file in append mode
    with open(log_file_path, 'a') as log_file:
        # Write a heading for the dataset
        log_file.write(f'Results for the dataset "{dataset_name}" are:\n')

        # Write the operation results
        log_file.write(f'Operation: {operation_name}\n')
        log_file.write(f'Mean execution time: {mean_exec_time:.3f} seconds\n')
        log_file.write(f'Standard deviation of execution time: {std_dev_exec_time:.3f} seconds\n')
        log_file.write(f'Mean space: {mean_size:.3f} MB\n')
        log_file.write(f'Standard deviation space: {std_dev_size:.3f} MB\n')
        log_file.write('-' * 50 + '\n')
        log_file.write('End of results for this dataset.\n')  
        log_file.write('=' * 50 + '\n\n') 

# Fucntion to extract number of noes and edges from the file name
def extract_file_info(file_name):
    """
    Extracts the number of nodes and edges from the filename.

    The filename is expected to be in the format `X_<num_nodes>_nodes_<num_edges>_edges.csv`.

    Parameters:
        filename (str): Path to the file with encoded information on nodes and edges.

    Returns:
        tuple: A tuple containing two integers:
            - num_nodes (int): The number of nodes extracted from the filename.
            - num_edges (int): The number of edges extracted from the filename.
    """

    match = re.search(r'(\d+)_nodes_(\d+)_edges', file_name)
    if match:
        num_nodes = int(match.group(1))
        num_edges = int(match.group(2))
        return num_nodes, num_edges
    else:
        raise ValueError(f"Filename format is not recognized: {file_name}")

# Function to create a database
def create_database(db_name):
    """
    Creates a new PostgreSQL database.

    Parameters:
    db_name (str): The name of the database to be created.
    """
    connection = psycopg2.connect(
        dbname='postgres',  # Connect to default db to create new db
        user='postgres',
        password='Berlin!321',
        host='localhost'
    )
    connection.autocommit = True

    cursor = connection.cursor()

    try:
        # Create the database
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database {db_name} created successfully.")
        
        # Connect to the newly created database and enable pg_stat_statements
        new_db_connection = psycopg2.connect(
            dbname=db_name,
            user='postgres',
            password='Berlin!321',
            host='localhost'
        )
        new_db_cursor = new_db_connection.cursor()
        
        new_db_cursor.close()
        new_db_connection.close()

    except psycopg2.Error as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        connection.close()


# Function to create tables in the PostgreSQL database with indexing and track index creation times
def create_tables_with_index(connection):
    """
    Creates the tables 'nodes', 'edges', and 'labels' in the connected PostgreSQL database, if they do not exist.
    Also adds indexes on certain columns to improve query performance.

    Parameters:
        connection (PostgreSQL connection object): Connection to the PostgreSQL database.

    Returns:
        tuple: A tuple containing two values:
            - mean_data_loading_index_time (float): Mean execution time for creating the indexes.
            - std_data_loading_index_time (float): Standard deviation of the execution time for creating the indexes.
    """

    cursor = connection.cursor()
    index_times = []

    # Function to execute an index creation and track the time
    def create_index_and_time(query):
        start_time = time.time()  # Start timing
        cursor.execute(query)     # Execute index creation
        connection.commit()       # Commit after each index creation
        end_time = time.time()    # End timing
        
        # Calculate and store the time taken
        time_taken = end_time - start_time
        index_times.append(time_taken)
    
    # Create the edges table and its indexes
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edges (
            source INT, 
            target INT
        )
    """)
    
    # Create indexes and track their creation times
    create_index_and_time("CREATE INDEX IF NOT EXISTS source_index ON edges (source)")
    create_index_and_time("CREATE INDEX IF NOT EXISTS target_index ON edges (target)")
    create_index_and_time("CREATE INDEX IF NOT EXISTS source_target_index ON edges (source, target)")
    create_index_and_time("CREATE INDEX IF NOT EXISTS target_source_index ON edges (target, source)")

    # Create the nodes table and its features
    cursor.execute("CREATE TABLE IF NOT EXISTS nodes (node_id SERIAL PRIMARY KEY)")
    
    # Dynamically add features if they do not exist
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='nodes'")
    columns = [column[0] for column in cursor.fetchall()]
    
    for i in range(100):  # Assuming 100 features
        feature_name = f"feature_{i}"
        if feature_name not in columns:
            cursor.execute(f"ALTER TABLE nodes ADD COLUMN {feature_name} FLOAT")

    # Create the labels table and its index
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS labels (
            node_id INT REFERENCES nodes(node_id)
        )
    """)
    create_index_and_time("CREATE INDEX IF NOT EXISTS node_id_index ON labels (node_id)")

    connection.commit()
    cursor.close()

    # Calculate mean and standard deviation for index creation times
    mean_data_loading_index_time = np.mean(index_times)
    std_data_loading_index_time = np.std(index_times)
    
    return mean_data_loading_index_time, std_data_loading_index_time


# Fucntion to extract execution time for upload operation
def extract_execution_time(explain_output):
    """
    Extracts the total execution time from the given query plan.

    The function parses the query plan to find the execution time, which is typically
    represented in the format 'Execution Time: X ms'. It returns the execution time
    in seconds.

    Parameters:
    query_plan (list): A list of strings representing the lines of the query plan output.

    Returns:
    float: The total execution time in seconds. If the execution time cannot be found,
           returns None.
    """

    execution_time = 0.0
    pattern = re.compile(r"Execution Time: (\d+\.\d+) ms")
    for row in explain_output:
        match = pattern.search(row[0])
        if match:
            execution_time += float(match.group(1))
    return execution_time / 1000  # Convert milliseconds to seconds

# Function to load edges data into PostgreSQL
def load_edges(connection, file_path, batch_size=30000):
    """
    Loads edges from a CSV file into the edges table in the database.

    Parameters:
    connection (psycopg2.extensions.connection): The database connection.
    edge_file (str): The path to the CSV file containing edge data.

    Returns:
    int: The number of edges loaded into the database.
    """

    print("Edges are loading!")
    cursor = connection.cursor()
    
    with open(file_path, 'r') as edges_file:
        edges_reader = csv.reader(edges_file)
        next(edges_reader)  # Skip header

        batch = []
        total_execution_time = 0.0

        for row in edges_reader:
            batch.append((int(row[0]), int(row[1])))

            if len(batch) >= batch_size:
                # Process the batch
                args_str = ','.join(cursor.mogrify("(%s,%s)", row).decode("utf-8") for row in batch)
                query = f"INSERT INTO edges (source, target) VALUES {args_str};"
                cursor.execute(f"EXPLAIN ANALYZE {query}")
                explain_output = cursor.fetchall()

                total_execution_time += extract_execution_time(explain_output)
                batch = []  # Clear batch

        # Insert remaining rows
        if batch:
            args_str = ','.join(cursor.mogrify("(%s,%s)", row).decode("utf-8") for row in batch)
            query = f"INSERT INTO edges (source, target) VALUES {args_str};"
            cursor.execute(f"EXPLAIN ANALYZE {query}")
            explain_output = cursor.fetchall()
            total_execution_time += extract_execution_time(explain_output)

    connection.commit()
    cursor.close()
    print("Edge loading completed!")

    return total_execution_time


# Function to load nodes data into PostgreSQL 
def load_nodes(connection, file_path, batch_size=30000):
    """
    Loads nodes from a CSV file into the nodes table in the database.

    Parameters:
    connection (psycopg2.extensions.connection): The database connection.
    node_file (str): The path to the CSV file containing node data.

    Returns:
    int: The number of nodes loaded into the database.
    """

    print("Nodes are loading!")
    cursor = connection.cursor()
    
    with open(file_path, 'r') as nodes_file:
        nodes_reader = csv.reader(nodes_file)
        next(nodes_reader)  # Skip header

        batch = []
        total_execution_time = 0.0

        for row in nodes_reader:
            batch.append(tuple(map(float, row)))

            if len(batch) >= batch_size:
                # Process the batch
                args_str = ','.join(
                    cursor.mogrify("(" + ",".join(["%s"] * 100) + ")", row).decode("utf-8")
                    for row in batch
                )
                query = f"INSERT INTO nodes ({', '.join([f'feature_{i}' for i in range(100)])}) VALUES {args_str};"
                cursor.execute(f"EXPLAIN ANALYZE {query}")
                explain_output = cursor.fetchall()

                total_execution_time += extract_execution_time(explain_output)
                batch = []  # Clear batch

        # Insert remaining rows
        if batch:
            args_str = ','.join(
                cursor.mogrify("(" + ",".join(["%s"] * 100) + ")", row).decode("utf-8")
                for row in batch
            )
            query = f"INSERT INTO nodes ({', '.join([f'feature_{i}' for i in range(100)])}) VALUES {args_str};"
            cursor.execute(f"EXPLAIN ANALYZE {query}")
            explain_output = cursor.fetchall()
            total_execution_time += extract_execution_time(explain_output)

    connection.commit()
    cursor.close()
    print("Node loading completed!")

    return total_execution_time


# Function to load labels data into PostgreSQL
def load_labels(connection, file_path, batch_size=30000):
    """
    Loads labels from a CSV file into the labels table in the database.

    Parameters:
    connection (psycopg2.extensions.connection): The database connection.
    label_file (str): The path to the CSV file containing label data.

    Returns:
    int: The number of labels loaded into the database.
    """

    print("Labels are loading!")
    cursor = connection.cursor()

    # Ensure the 'label' column exists
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='labels'")
    existing_columns = set([column[0] for column in cursor.fetchall()])
    if "label" not in existing_columns:
        cursor.execute("ALTER TABLE labels ADD COLUMN label TEXT")
    
    with open(file_path, 'r') as labels_file:
        labels_reader = csv.reader(labels_file)
        next(labels_reader)  # Skip header

        batch = []
        total_execution_time = 0.0

        for idx, row in enumerate(labels_reader, start=1):
            batch.append((idx, row[0]))

            if len(batch) >= batch_size:
                # Process the batch
                args_str = ','.join(cursor.mogrify("(%s,%s)", row).decode("utf-8")
                                    for row in batch)
                query = f"INSERT INTO labels (node_id, label) VALUES {args_str};"
                cursor.execute(f"EXPLAIN ANALYZE {query}")
                explain_output = cursor.fetchall()

                total_execution_time += extract_execution_time(explain_output)
                batch = []  # Clear batch

        # Insert remaining rows
        if batch:
            args_str = ','.join(cursor.mogrify("(%s,%s)", row).decode("utf-8")
                                for row in batch)
            query = f"INSERT INTO labels (node_id, label) VALUES {args_str};"
            cursor.execute(f"EXPLAIN ANALYZE {query}")
            explain_output = cursor.fetchall()
            total_execution_time += extract_execution_time(explain_output)

    connection.commit()
    cursor.close()
    print("Label loading completed!")

    return total_execution_time

# Function to get table sizes to calculate space
def get_table_sizes(connection):
    """
    Retrieves the sizes of all tables within the current PostgreSQL database.

    This function queries the information schema to obtain the size of each table
    in the database in terms of disk space used by data and indexes, and returns 
    this information in a readable format.

    Parameters:
        connection (PostgreSQL connection object): Connection to the PostgreSQL database.

    Returns:
        dict: A dictionary where each key is the name of a table, and the value is the size in megabytes (MB).
    """

    cursor = connection.cursor()
    cursor.execute("""
        SELECT
            table_name,
            pg_total_relation_size(quote_ident(table_name)) / (1024 * 1024) AS total_size_mb
        FROM
            information_schema.tables
        WHERE
            table_schema = 'public';
    """)
    table_sizes = dict(cursor.fetchall())
    cursor.close()
    return table_sizes

# Function to measure upload time and space taken by tables in PostgreSQL
def measure_performance(connection, operation_function, num_iterations, file_path):
    """
    Measures the performance of a specified database operation.

    Parameters:
    connection (psycopg2.extensions.connection): The database connection.
    operation (function): The operation to be measured (e.g., load_edges).
    num_iterations (int): The number of iterations to run the measurement.
    file_path (str): The path to the data file used for the operation.

    Returns:
    tuple: Mean execution time, standard deviation of execution time, mean space usage, and standard deviation of space usage.
    """

    times = []
    spaces = []

    for iteration in range(num_iterations):
        # Measure space before operation
        space_before = get_table_sizes(connection)

        # Execute operation and measure time
        execution_time = operation_function(connection, file_path)

        # Measure space after operation
        space_after = get_table_sizes(connection)

        # Calculate space usage
        total_space_used = sum(space_after.get(table, 0) - space_before.get(table, 0) for table in space_after)
        
        times.append(execution_time)
        spaces.append(total_space_used)

    mean_time = np.mean(times)
    std_dev_time = np.std(times)
    mean_space = np.mean(spaces)
    std_dev_space = np.std(spaces)

    return mean_time, std_dev_time, mean_space, std_dev_space

# Helper function to extract execution time from EXPLAIN ANALYZE output
def extract_execution_time_from_plan(query_plan):
    """
    Extracts the execution time from the query plan returned by PostgreSQL.

    This function scans the execution plan to locate the line containing the execution
    time information. The expected format is 'Execution Time: X ms', and the function
    converts this value to seconds before returning it.

    Parameters:
    query_plan (list): The execution plan returned by PostgreSQL after executing a query.

    Returns:
    float: The execution time in seconds, or None if the execution time cannot be found
           in the query plan.
    """

    total_time = 0.0
    pattern = re.compile(r"Execution Time: (\d+\.\d+) ms")
    
    for line in query_plan:
        line_text = line[0]  # Fetch the text from the tuple
        match = pattern.search(line_text)
        if match:
            total_time += float(match.group(1))
    
    return total_time / 1000  # Convert milliseconds to seconds



# Function to perform read operation 
def read(connection, num_hops, num_iterations):
    """
    Execute a read operation to retrieve subgraph data from a database.

    This function creates temporary tables for subgraph nodes and edges, performs hops 
    to explore the graph, and creates indexes. It measures execution and index creation 
    times across multiple iterations.

    Parameters:
        connection (object): Database connection for executing SQL commands.
        num_hops (int): Number of hops to traverse in the graph.
        num_iterations (int): Number of times to perform the read operation.

    Returns:
        tuple: Contains mean and standard deviation of execution times and index creation times:
            - mean_execution_time (float)
            - std_dev_execution_time (float)
            - mean_index_creation_time (float)
            - std_dev_index_creation_time (float)

    Raises:
        Exception: If any SQL execution fails.

    """
    print("Read operation starting!")
    execution_times = []
    index_creation_times = []  # List to store index creation times

    for _ in range(num_iterations):
        cursor = connection.cursor()
        
        # Measure the execution time for the entire subgraph operation in this iteration
        total_execution_time = 0.0
        total_index_creation_time = 0.0  # To track index creation time for this iteration

        # Create a unique temporary table for subgraph nodes
        temp_table_nodes = f"temp_subgraph_nodes_{_}"
        cursor.execute(f"""
        EXPLAIN ANALYZE
        CREATE TEMP TABLE {temp_table_nodes} AS (
            SELECT n.node_id
            FROM nodes n
            JOIN (
                SELECT DISTINCT source AS node_id FROM edges
                UNION
                SELECT DISTINCT target AS node_id FROM edges
            ) sub ON n.node_id = sub.node_id
        )
        """)
        query_plan = cursor.fetchall()
        total_execution_time += extract_execution_time_from_plan(query_plan)

        # Create a remapping table for new indices
        remapping_table = f"temp_node_remap_{_}"
        cursor.execute(f"""
        EXPLAIN ANALYZE
        CREATE TEMP TABLE {remapping_table} AS (
            SELECT node_id, ROW_NUMBER() OVER () - 1 AS remapped_id
            FROM {temp_table_nodes}
        )
        """)
        query_plan = cursor.fetchall()
        total_execution_time += extract_execution_time_from_plan(query_plan)

        # Perform hops to get the desired neighborhood of each source node
        for depth in range(num_hops):
            cursor.execute(f"""
            EXPLAIN ANALYZE
            INSERT INTO {temp_table_nodes} (node_id)
            SELECT DISTINCT e.target
            FROM edges e
            JOIN {temp_table_nodes} sub_nodes ON e.source = sub_nodes.node_id
            WHERE e.target IN (
                SELECT node_id
                FROM {temp_table_nodes}
            )
            """)
            query_plan = cursor.fetchall()
            total_execution_time += extract_execution_time_from_plan(query_plan)

        # Create a unique temporary table for subgraph edges using remapped indices
        temp_table_edges = f"temp_subgraph_edge_index_{_}"
        cursor.execute(f"""
        EXPLAIN ANALYZE
        CREATE TEMP TABLE {temp_table_edges} AS (
            SELECT r_src.remapped_id AS source, r_tgt.remapped_id AS target
            FROM edges e
            JOIN {remapping_table} r_src ON e.source = r_src.node_id
            JOIN {remapping_table} r_tgt ON e.target = r_tgt.node_id
            WHERE r_src.node_id IN (SELECT node_id FROM {temp_table_nodes})
              AND r_tgt.node_id IN (SELECT node_id FROM {temp_table_nodes})
        )
        """)
        query_plan = cursor.fetchall()
        total_execution_time += extract_execution_time_from_plan(query_plan)

        # Create composite indexes on the edges table and track the creation time
        start_time = time.time()  # Start time for index creation
        cursor.execute(f"CREATE INDEX source_target_index_{_} ON {temp_table_edges} (source, target)")
        cursor.execute(f"CREATE INDEX target_source_index_{_} ON {temp_table_edges} (target, source)")
        connection.commit()  # Commit index creation
        index_creation_time = time.time() - start_time  # Calculate time taken
        index_creation_times.append(index_creation_time)  # Store index creation time
        total_index_creation_time += index_creation_time

        # Perform additional hops on the edges table
        for depth in range(num_hops):
            cursor.execute(f"""
            EXPLAIN ANALYZE
            INSERT INTO {temp_table_edges} (source, target)
            SELECT DISTINCT r_src.remapped_id, r_tgt.remapped_id
            FROM edges e
            JOIN {temp_table_edges} se ON e.source = se.target
            JOIN {remapping_table} r_src ON e.source = r_src.node_id
            JOIN {remapping_table} r_tgt ON e.target = r_tgt.node_id
            WHERE r_src.node_id IN (
                SELECT DISTINCT source
                FROM {temp_table_edges}
            )
            """)
            query_plan = cursor.fetchall()
            total_execution_time += extract_execution_time_from_plan(query_plan)

        execution_times.append(total_execution_time)

        # Drop temporary tables to free up space
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_nodes} CASCADE;")
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_edges} CASCADE;")
        cursor.execute(f"DROP TABLE IF EXISTS {remapping_table} CASCADE;")
        connection.commit()

        cursor.close()

    # Calculate mean and standard deviation of execution times
    mean_execution_time = np.mean(execution_times)
    std_dev_execution_time = np.std(execution_times)

    # Calculate mean and standard deviation of index creation times
    mean_index_creation_time = np.mean(index_creation_times)
    std_dev_index_creation_time = np.std(index_creation_times)

    print("Read operation completed!")
    return mean_execution_time, std_dev_execution_time, mean_index_creation_time, std_dev_index_creation_time

# Function to update node features with random values and measure performance
def update_node_features(connection, num_iterations):
    """
    Updates the features of nodes in the database with random values.

    Parameters:
    connection (psycopg2.extensions.connection): The database connection.
    num_iterations (int): The number of iterations to perform the updates.

    Returns:
    tuple: Mean execution time and standard deviation of execution time for the updates.
    """

    execution_times = []
    cursor = connection.cursor()

    for _ in range(num_iterations):
        try:
            # Generate random feature values
            features = [random.random() for _ in range(50)]
            
            # Construct the update query
            update_query = """
            EXPLAIN ANALYZE
            UPDATE nodes
            SET {updates}
            WHERE node_id = %s;
            """.format(updates=', '.join([f"feature_{i} = %s" for i in range(50)]))
            
            # Fetch all node IDs to iterate over
            cursor.execute("SELECT node_id FROM nodes;")
            node_ids = cursor.fetchall()

            total_execution_time = 0.0
            
            # Update node features for each node
            for node_id in node_ids:
                node_id = node_id[0]
                cursor.execute(update_query, (*features, node_id))
                
                # Measure the time taken to execute the update
                query_plan = cursor.fetchall()
                execution_time = extract_execution_time_from_plan(query_plan)
                total_execution_time += execution_time
            
            execution_times.append(total_execution_time)

            # Commit changes
            connection.commit()
            print("Node features updated successfully.")

        except psycopg2.Error as e:
            print(f"Error updating node features: {e}")
            connection.rollback()
    
    cursor.close()

    if execution_times:
        mean_time = np.mean(execution_times)
        std_dev_time = np.std(execution_times)
    else:
        mean_time = std_dev_time = float('nan')

    return mean_time, std_dev_time


# Function to add a unique edge ID and weight column to the edges table
def add_weight_column(connection):
    """
    Adds a weight column to the edges table if it doesn't already exist.

    Parameters:
    connection (psycopg2.extensions.connection): The database connection.
    """

    cursor = connection.cursor()
    try:
        # Add the edge_id and weight columns if they don't exist
        cursor.execute("ALTER TABLE edges ADD COLUMN IF NOT EXISTS weight FLOAT;")
        connection.commit()
    except psycopg2.Error as e:
        print(f"Error adding weight column: {e}")
        connection.rollback()
    finally:
        cursor.close()

# Function to update 5% of edges with random weights and measure performance
def update_edge_weights(connection, num_iterations):
    """
    Updates 5% of the edges in the database with random weights.

    Parameters:
    connection (psycopg2.extensions.connection): The database connection.
    num_iterations (int): The number of iterations to perform the updates.

    Returns:
    tuple: Mean execution time and standard deviation of execution time for the updates.
    """

    execution_times = []
    percent = 5
    
    for _ in range(num_iterations):
        cursor = connection.cursor()

        try:
            # Ensure the weight column exists
            add_weight_column(connection)

            # Calculate the number of edges to update
            cursor.execute("SELECT COUNT(*) FROM edges;")
            total_edges = cursor.fetchone()[0]
            num_edges_to_update = int(total_edges * percent / 100)

            # Update 5% of edges with random weights
            cursor.execute("""
            EXPLAIN ANALYZE
            WITH edge_ids AS (
                SELECT ctid
                FROM edges
                ORDER BY source, target  -- Sorting to match Neo4j consistency
                LIMIT %s
            )
            UPDATE edges
            SET weight = %s
            WHERE ctid IN (SELECT ctid FROM edge_ids);
            """, (num_edges_to_update, random.uniform(0, 1)))

            # Fetch the query plan which includes execution time
            query_plan = cursor.fetchall()
            execution_time = extract_execution_time_from_plan(query_plan)
            if execution_time is not None:
                execution_times.append(execution_time)
            else:
                print("No execution time found in the query plan.")
            
            # Commit changes
            connection.commit()
            print("Edge weights updated successfully.")

        except psycopg2.Error as e:
            print(f"Error updating edge weights: {e}")
            connection.rollback()

        finally:
            cursor.close()

    if execution_times:
        mean_time = np.mean(execution_times)
        std_dev_time = np.std(execution_times)
    else:
        mean_time = std_dev_time = float('nan')

    return mean_time, std_dev_time 


# Function to delete all data from tables and measure performance (measure time using "time.perf_counter()" as EXPLAIN ANALYZE doesn't support "TRUNCATE" operation)
def delete_all(connection, num_iterations):
    """
    Deletes all data from the nodes, edges, and labels tables.

    Parameters:
    connection (psycopg2.extensions.connection): The database connection.
    num_iterations (int): The number of iterations to perform the deletions.

    Returns:
    tuple: Mean execution time and standard deviation of execution time for the deletions.
    """

    execution_times = []
    cursor = connection.cursor()

    for _ in range(num_iterations):
        try:
            start_time = time.perf_counter()  # Start timing

            # Execute TRUNCATE command
            cursor.execute("TRUNCATE TABLE edges, nodes, labels CASCADE;")
            connection.commit()

            end_time = time.perf_counter()  # End timing

            # Calculate elapsed time
            execution_time = end_time - start_time
            execution_times.append(execution_time)

            print("All data deleted successfully.")

        except psycopg2.Error as e:
            print(f"Error deleting data: {e}")
            connection.rollback()
    
    cursor.close()

    if execution_times:
        mean_time = np.mean(execution_times)
        std_dev_time = np.std(execution_times)
    else:
        mean_time = std_dev_time = float('nan')

    return mean_time, std_dev_time

#  Main block
if __name__ == "__main__":
    """
    Main execution block of the script.

    This block initializes the parameters, creates the database and tables,
    loads data from CSV files, measures performance for various operations,
    and logs the results.

    The following operations are performed:
    - Loading edges, nodes, and labels.
    - Reading data with a specified number of hops.
    - Updating node features and edge weights.
    - Deleting all data.
    """

    # Parameters
    num_iterations = 10
    num_hops = 3

    # Define file paths and database names based on files    
    edge_file = 'E:/Master Thesis/Thesis_Code/data/edge_index_100000_nodes_5_edges.csv'
    node_file = 'E:/Master Thesis/Thesis_Code/data/X_100000_nodes_5_edges.csv'
    label_file = 'E:/Master Thesis/Thesis_Code/data/y_100000_nodes_5_edges.csv'    

    nodes_count, edges_count = extract_file_info(node_file)
    db_name = f"query_ops_{nodes_count}_nodes_{edges_count}_edges_{num_iterations}_iter"
    dataset_name = f"dataset_{nodes_count}_nodes_{edges_count}_edges"


    # Create a new database
    create_database(db_name)

    # Connect to the newly created database
    connection = psycopg2.connect(
        dbname=db_name,
        user='postgres',
        password='Berlin!321',
        host='localhost'
    )

    # Create tables with indexing
    create_tables_with_index(connection)

    # Print results
    # Measure performance for loading edges
    print(f"Results of CRUD operation on {dataset_name} in PostgreSQL are:")

    print("Edges Upload Performance:")
    mean_time, std_dev_time, mean_space, std_dev_space = measure_performance(connection, load_edges, num_iterations, edge_file)
    print(f"Mean Execution Time: {mean_time:.3f} seconds, Std Dev Time: {std_dev_time:.3f} seconds")
    print(f"Mean Space Usage: {mean_space:.3f} MB, Std Dev Space: {std_dev_space:.3f} MB")
    log_results(db_name, "Upload Edges", mean_time, std_dev_time, mean_space, std_dev_space, log_file_path)


    # Measure performance for loading nodes
    print("Nodes Upload Performance:")
    mean_time, std_dev_time, mean_space, std_dev_space = measure_performance(connection, load_nodes, num_iterations, node_file)
    print(f"Mean Execution Time: {mean_time:.3f} seconds, Std Dev Time: {std_dev_time:.3f} seconds")
    print(f"Mean Space Usage: {mean_space:.3f} MB, Std Dev Space: {std_dev_space:.3f} MB")
    log_results(db_name, "Upload Nodes", mean_time, std_dev_time, mean_space, std_dev_space, log_file_path)


    # Measure performance for loading labels
    print("Labels Upload Performance:")
    mean_time, std_dev_time, mean_space, std_dev_space = measure_performance(connection, load_labels, num_iterations, label_file)
    print(f"Mean Execution Time: {mean_time:.3f} seconds, Std Dev Time: {std_dev_time:.3f} seconds")
    print(f"Mean Space Usage: {mean_space:.3f} MB, Std Dev Space: {std_dev_space:.3f} MB")
    log_results(db_name, "Upload Labels", mean_time, std_dev_time, mean_space, std_dev_space, log_file_path)

    # Data loading and index timing
    mean_data_loading_index_time, std_data_loading_index_time  = create_tables_with_index(connection)

    # Measure performance for read operation
    print("Read Operation Performance:")
    mean_time, std_dev_time,  mean_read_index_time, std_dev_read_index_time = read(connection, num_hops, num_iterations)
    print(f"Mean time: {mean_time:.3f} seconds, Std Dev time: {std_dev_time:.3f} seconds")
    log_results(db_name, "Read", mean_time, std_dev_time, 0, 0, log_file_path)


    # Combine index times
    print("Index Creation Performance:")
    total_index_mean_time = mean_data_loading_index_time + mean_read_index_time
    total_index_std_time = np.sqrt(std_data_loading_index_time**2 + std_dev_read_index_time**2)
    print(f"Mean index creation time: {total_index_mean_time:.3f} seconds, Std Dev index creation time: {total_index_std_time:.3f} seconds")                                   
    log_results(db_name, "Total Index Creation Time", total_index_mean_time, total_index_std_time, 0, 0, log_file_path)


    # Measure performance for updating node features
    print("Node Update Performance:")
    mean_time, std_dev_time = update_node_features(connection, num_iterations)
    print(f"Mean time: {mean_time:.3f} seconds, Std Dev time: {std_dev_time:.3f} seconds")
    log_results(db_name, "Node Update", mean_time, std_dev_time, 0, 0, log_file_path)

    # Measure performance for updating edge weights
    print("Edge Weight Update Performance:")
    mean_time, std_dev_time = update_edge_weights(connection, num_iterations)
    print(f"Mean time: {mean_time:.3f} seconds, Std Dev time: {std_dev_time:.3f} seconds")
    log_results(db_name, "Edge Update", mean_time, std_dev_time, 0, 0, log_file_path)

    # Measure performance for deleting all data
    print("Delete All Performance:")
    mean_time, std_dev_time = delete_all(connection, num_iterations)
    print(f"Mean time: {mean_time:.3f} seconds, Std Dev time: {std_dev_time:.3f} seconds")
    log_results(db_name, "Delete", mean_time, std_dev_time, 0, 0, log_file_path)

    connection.close()

    print("All operations completed successfully.")
