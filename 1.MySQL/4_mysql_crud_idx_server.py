
"""
MySQL CRUD Operations and Performance Benchmarking Script 

This script is designed to perform various CRUD (Create, Read, Update, Delete) operations on a MySQL database
and to measure the performance of these operations on large datasets in batches on the server. It automates the following steps:

1. Database and Table Creation: 
   - Dynamically creates a database based on file information and iterates through multiple CRUD operations.
   - Creates tables for nodes, edges, and labels, each with specified indices for optimized query performance.

2. Data Loading:
   - Loads node, edge, and label data batch-wise from CSV files into the database.
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
Before running, ensure MySQL is running and accessible with the specified credentials. Execute the script from the command line 
or an IDE, providing any necessary file paths and configurations within the main block.

Dependencies:
- `mysql.connector`: For MySQL database connectivity
- `numpy`: For calculating mean and standard deviation
- `os`: For file size operations
- `csv`: For reading CSV files

"""

# Import required libraries
import mysql.connector
import numpy as np
import logging
import random
import time
import os
import csv
import sys

def flush_log():
    """
    Flushes all logging handlers to write buffered log messages.

    This function iterates through all handlers attached to the root logger
    and calls their `flush()` method, ensuring timely log reporting.

    Returns:
    None
    """

    for handler in logging.getLogger().handlers:
        handler.flush()

# Function to extract information from file names
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
        
    base_name = os.path.basename(file_name)  # Get the base filename from the path
    parts = base_name.split('_')
    
    num_nodes = int(parts[1])  # Extracting the number of nodes
    num_edges = int(parts[3])  # Extracting the number of edges
    
    return num_nodes, num_edges


# Function to create tables in the MySQL database with indexing and track index creation times
def create_tables_with_index(connection):
    """
    Creates the tables 'nodes', 'edges', and 'labels' in the connected MySQL database, if they do not exist.
    Also adds indexes on certain columns to improve query performance.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.

    Returns:
        tuple: A tuple containing two values:
            - mean_data_loading_index_time (float): Mean execution time for creating the indexes.
            - std_data_loading_index_time (float): Standard deviation of the execution time for creating the indexes.
    """

    cursor = connection.cursor()
    index_times = []

    # Function to check for an index and create it if it does not exist
    def create_index_and_time(index_name, table_name, column_name):
        # Check if the index already exists
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.statistics
            WHERE table_schema = DATABASE() 
              AND table_name = '{table_name}' 
              AND index_name = '{index_name}'
        """)
        index_exists = cursor.fetchone()[0]

        # Only create the index if it does not exist
        if index_exists == 0:
            start_time = time.time()  # Start timing
            try:
                cursor.execute(f"CREATE INDEX {index_name} ON {table_name} ({column_name})")
                connection.commit()
                end_time = time.time()  # End timing
                index_times.append(end_time - start_time)  # Store the time taken
            except Exception as e:
                print(f"Error creating index {index_name}: {e}")

    # Step 1: Create the `edges` table without indexes initially
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edges (
            source INT, 
            target INT
        )
    """)

    # Step 2: Add indexes to the `edges` table, ensuring they don't already exist
    create_index_and_time("source_index", "edges", "source")
    create_index_and_time("target_index", "edges", "target")
    create_index_and_time("source_target_index", "edges", "source, target")
    create_index_and_time("target_source_index", "edges", "target, source")

    # Step 3: Create the `nodes` table with a primary key on `node_id`
    cursor.execute("CREATE TABLE IF NOT EXISTS nodes (node_id INT AUTO_INCREMENT PRIMARY KEY)")

    # Step 4: Dynamically add 100 feature columns if they do not already exist
    cursor.execute("SHOW COLUMNS FROM nodes")
    columns = [column[0] for column in cursor.fetchall()]

    for i in range(100):  # Assuming 100 features
        feature_name = f"feature_{i}"
        if feature_name not in columns:
            cursor.execute(f"ALTER TABLE nodes ADD COLUMN {feature_name} FLOAT")

    # Step 5: Create the `labels` table with a `label` column and add an index on `node_id`
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS labels (
            node_id INT,
            label VARCHAR(255),   -- Re-add the `label` column here
            INDEX node_id_index (node_id)
        )
    """)

    # Commit changes and close the cursor
    connection.commit()
    cursor.close()

    # Step 6: Calculate the mean and standard deviation for index creation times
    if index_times:  # Only calculate if index_times is not empty
        mean_data_loading_index_time = np.mean(index_times)
        std_data_loading_index_time = np.std(index_times)

    else:
        mean_data_loading_index_time = 0  
        std_data_loading_index_time = 0  
        print("No indexes created. Mean and Std Dev cannot be calculated.")

    return mean_data_loading_index_time, std_data_loading_index_time


# Function to load edges data into MySQL in batches
def load_edges(connection, file_path, batch_size=30000):
    """
    Loads edge data batch-wise from a CSV file into the 'edges' table in the database.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.
        edge_file (str): Path to the CSV file containing edge data.

    Returns:
        None
    """

    logging.info("Edges are loading!")
    with open(file_path, 'r') as edges_file:
        edges_reader = csv.reader(edges_file)
        next(edges_reader)  # Skip header

        cursor = connection.cursor()
        batch = []
        for idx, row in enumerate(edges_reader, start=1):
            source, target = map(int, row)
            batch.append((source, target))
            
            if idx % batch_size == 0:
                cursor.executemany("INSERT INTO edges (source, target) VALUES (%s, %s)", batch)
                connection.commit()
                batch = []

        if batch:
            cursor.executemany("INSERT INTO edges (source, target) VALUES (%s, %s)", batch)
            connection.commit()

        logging.info("Edge loading completed!")
        flush_log()
        cursor.close()

# Function to load nodes data into MySQL in batches
def load_nodes(connection, file_path, batch_size=30000):
    """
    Loads node data batch-wise from a CSV file into the 'nodes' table in the database.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.
        node_file (str): Path to the CSV file containing node data.

    Returns:
        None
    """

    logging.info("Nodes are loading!")
    with open(file_path, 'r') as nodes_file:
        nodes_reader = csv.reader(nodes_file)
        next(nodes_reader)  # Skip header

        cursor = connection.cursor()

        query = "INSERT INTO nodes ("
        for i in range(100):  
            query += f"feature_{i}"
            if i < 99:
                query += ","
        query += ") VALUES ("
        query += ",".join(["%s"] * 100)
        query += ")"

        batch = []
        for idx, row in enumerate(nodes_reader, start=1):
            node_data = list(map(float, row))
            batch.append(tuple(node_data))
            
            if idx % batch_size == 0:
                cursor.executemany(query, batch)
                connection.commit()
                batch = []

        if batch:
            cursor.executemany(query, batch)
            connection.commit()

        logging.info("Node loading completed!")
        flush_log()
        cursor.close()

# Function to load labels data into MySQL in batches
def load_labels(connection, file_path, batch_size=30000):
    """
    Loads label data batch-wise from a CSV file into the 'labels' table in the database.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.
        label_file (str): Path to the CSV file containing label data.

    Returns:
        None
    """

    logging.info("Labels are loading!")
    with open(file_path, 'r') as labels_file:
        labels_reader = csv.reader(labels_file)
        next(labels_reader)  # Skip header

        cursor = connection.cursor()

        batch = []
        for idx, row in enumerate(labels_reader, start=1):
            label = row[0]
            batch.append((idx, label))
            
            if idx % batch_size == 0:
                cursor.executemany("INSERT INTO labels (node_id, label) VALUES (%s, %s)", batch)
                connection.commit()
                batch = []

        if batch:
            cursor.executemany("INSERT INTO labels (node_id, label) VALUES (%s, %s)", batch)
            connection.commit()

        logging.info("Label loading completed!")
        flush_log()
        cursor.close()

# Function to get table size to calculate space
def get_table_sizes(connection):
    """
    Retrieves the sizes of all tables within the current MySQL database.

    This function queries the information schema to obtain the size of each table
    in the database in terms of disk space used by data and indexes, and returns 
    this information in a readable format.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.

    Returns:
        dict: A dictionary where each key is the name of a table, and the value is the size in megabytes (MB).

    Raises:
        mysql.connector.Error: If there is an error executing the query or fetching results.
    """

    cursor = connection.cursor()
    cursor.execute("SHOW TABLE STATUS")
    table_sizes = {}
    for table_info in cursor.fetchall():
        table_name = table_info[0]
        data_length = table_info[6] / (1024 * 1024)  # Convert data_length to MB
        total_size = data_length 
        table_sizes[table_name] = total_size
    cursor.close()
    return table_sizes

# Function to measure upload time and space taken by tables in MySQL
def measure_performance(connection, operation_function, num_iterations, *args, **kwargs):
    """
    Measures the performance of a given database operation, including execution time and space usage.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.
        func (function): The function to measure.
        num_iterations (int): The number of times to repeat the operation.
        *args: Additional arguments to pass to the function being measured.

    Returns:
        tuple: A tuple containing four values:
            - mean_time (float): Mean execution time of the operation over the iterations.
            - std_dev_time (float): Standard deviation of the execution time.
            - mean_space (float): Mean space usage during the operation.
            - std_dev_space (float): Standard deviation of the space usage.
    """

    times = []
    spaces = []

    for _ in range(num_iterations):
        cursor = connection.cursor()
        cursor.execute("SET profiling = 1;")

        # Perform operation with additional arguments
        operation_function(connection, *args, **kwargs)

        cursor.execute("SHOW PROFILES;")
        profiles = cursor.fetchall()
        elapsed_time = profiles[-1][1]  # Use the duration of the last profile

        times.append(elapsed_time)

        # Measure space after operation
        table_sizes = get_table_sizes(connection)
        total_data_used = sum(table_sizes.values())
        spaces.append(total_data_used)    

        cursor.execute("SET profiling = 0;")
        cursor.close()

    mean_time = np.mean(times)  
    std_dev_time = np.std(times)  

    mean_space = np.mean(spaces)
    std_space = np.std(spaces) 

    return mean_time, std_dev_time, mean_space, std_space


# Function to perform read operation
def read(connection, num_hops, num_iterations):
    """
    Performs a read operation from the database and measures its performance over a given number of iterations.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.
        num_hops (int): The number of hops to traverse in the graph data.
        num_iterations (int): Number of times to perform the read operation.

    Returns:
        tuple: A tuple containing four values:
            - mean_time (float): Mean execution time for the read operation.
            - std_dev_time (float): Standard deviation of the execution time.
            - mean_read_index_time (float): Mean index read time.
            - std_read_index_time (float): Standard deviation of the index read time.
    """

    logging.info("Read operation starting!")
    execution_times = []
    read_index_time = []  # Initialize list for read index times

    for iteration in range(num_iterations):
        cursor = connection.cursor()
        try:
            # Enable profiling
            cursor.execute("SET profiling = 1;")

            # Step 1: Create a unique temporary table for subgraph nodes
            temp_table_nodes = f"temp_subgraph_nodes_{iteration}"
            cursor.execute(f"""
            CREATE TABLE {temp_table_nodes} AS (
                SELECT n.node_id
                FROM nodes n
                JOIN (
                    SELECT DISTINCT source AS node_id FROM edges
                    UNION
                    SELECT DISTINCT target AS node_id FROM edges
                ) sub ON n.node_id = sub.node_id
            )
            """)

            # Step 2: Perform hops to get the desired neighborhood of each source node
            for depth in range(num_hops):
                cursor.execute(f"""
                INSERT IGNORE INTO {temp_table_nodes} (node_id)
                SELECT DISTINCT target
                FROM edges e
                JOIN {temp_table_nodes} sub_nodes ON e.source = sub_nodes.node_id
                WHERE target NOT IN (SELECT node_id FROM {temp_table_nodes})
                """)

            # Step 3: Create a mapping of node id to updated index
            remapping_table = f"temp_node_remap_{iteration}"
            cursor.execute(f"""
            CREATE TABLE {remapping_table} AS (
                SELECT node_id, ROW_NUMBER() OVER () - 1 AS remapped_id
                FROM {temp_table_nodes}
            )
            """)

            # Step 4: Create a unique temporary table for subgraph edges using remapped indices
            temp_table_edges = f"temp_subgraph_edge_index_{iteration}"
            cursor.execute(f"""
            CREATE TABLE {temp_table_edges} AS (
                SELECT r_src.remapped_id AS source, r_tgt.remapped_id AS target
                FROM edges e
                JOIN {remapping_table} r_src ON e.source = r_src.node_id
                JOIN {remapping_table} r_tgt ON e.target = r_tgt.node_id
            )
            """)

            # Step 5: Track time for each index creation separately
            for index_creation in [
                ("source_target_index", "(source, target)"), 
                ("target_source_index", "(target, source)")
            ]:
                index_name, index_columns = index_creation
                start_time = time.time()

                # Try creating the index and catch any errors
                try:
                    cursor.execute(f"CREATE INDEX {index_name}_{iteration} ON {temp_table_edges} {index_columns}")
                    connection.commit()  # Commit the index creation
                except Exception as e:
                    logging.info(f"Error creating index {index_name}_{iteration}: {e}")
                    read_index_time.append(None)  # Append None instead of 0 to mark failure
                    continue  # Skip to the next index creation

                end_time = time.time()
                elapsed_time = end_time - start_time
                read_index_time.append(elapsed_time)  # Record index creation time

            # Step 6: Show profiling information
            cursor.execute("SHOW PROFILES;")
            profiles = cursor.fetchall()

            # Calculate total time for all queries executed during this iteration
            total_time = sum(profile[1] for profile in profiles)
            execution_times.append(total_time)  # Append the execution time

            # Drop temporary tables to free up space
            cursor.execute(f"DROP TABLE {temp_table_nodes}")
            cursor.execute(f"DROP TABLE {temp_table_edges}")
            cursor.execute(f"DROP TABLE {remapping_table}")

            cursor.execute("SET profiling = 0;")
            connection.commit()

        except Exception as e:
            logging.info(f"An error occurred in iteration {iteration}: {e}")
            connection.rollback()
        finally:
            cursor.close()

    # Calculate mean execution time and standard deviation
    if execution_times:
        mean_time = np.mean(execution_times)
        std_dev_time = np.std(execution_times)
    else:
        mean_time = None
        std_dev_time = None
        logging.info("No execution times recorded. Mean and standard deviation cannot be calculated.")

    # Calculate mean and standard deviation of index creation times
    if read_index_time:
        # Filter out None values and ensure the list is valid for calculations
        valid_index_times = [t for t in read_index_time if t is not None]
        if valid_index_times:
            mean_read_index_time = np.mean(valid_index_times)
            std_read_index_time = np.std(valid_index_times)
        else:
            mean_read_index_time = None
            std_read_index_time = None
    else:
        mean_read_index_time = None
        std_read_index_time = None
        logging.info("No index creation times recorded. Mean and standard deviation cannot be calculated.")

    logging.info("Read operation completed!")
    return mean_time, std_dev_time, mean_read_index_time, std_read_index_time



# Function to update nodes in batches with random values
def update_nodes(connection, num_iterations, batch_size=30000):
    """
    Updates certain attributes of nodes in the 'nodes' table and measures execution time.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.
        num_iterations (int): The number of times to perform the update operation.

    Returns:
        tuple: A tuple containing two values:
            - mean_time (float): Mean execution time for the node update operation.
            - std_dev_time (float): Standard deviation of the execution time.
    """

    logging.info("Node update operation starting!")
    
    cursor = connection.cursor()
    execution_times = []

    try:
        for iteration in range(num_iterations):
            logging.info(f"Starting iteration {iteration + 1}/{num_iterations}")
            
            # Enable profiling to measure execution time
            cursor.execute("SET profiling = 1;")
            
            # Determine the total number of nodes in the 'nodes' table
            cursor.execute("SELECT COUNT(*) FROM nodes")
            total_nodes = cursor.fetchone()[0]
            
            # Generate random values for each node_id
            random_updates = [(random.random(), node_id) for node_id in range(1, total_nodes + 1)]
            
            # Update nodes in batches
            for start in range(0, total_nodes, batch_size):
                end = start + batch_size
                batch = random_updates[start:end]
                cursor.executemany("UPDATE nodes SET feature_0 = %s WHERE node_id = %s", batch)
                connection.commit()
            
            # Get the execution time from the profile of this iteration
            cursor.execute("SHOW PROFILES;")
            profiles = cursor.fetchall()
            iteration_time = profiles[-1][1]  # Use the duration of the last profile entry
            execution_times.append(iteration_time)
            logging.info(f"Iteration {iteration + 1} completed with execution time: {iteration_time:.3f} seconds")
            
            # Disable profiling after each iteration
            cursor.execute("SET profiling = 0;")
        
    finally:
        logging.info("Node update operation completed!")
        cursor.close()

    if execution_times:
        mean_time = np.mean(execution_times)
        std_dev_time = np.std(execution_times)
        return mean_time, std_dev_time
    else:
        return float('nan'), float('nan')  # Return NaN if no iterations were successful


# Function to add a weight column to the edges table
def add_weight_column(connection):
    """
    Adds a 'weight' column to the 'edges' table if it does not already exist.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.

    Returns:
        None
    """

    logging.info("Adding weight column!")
    cursor = connection.cursor()
    try:
        cursor.execute("SHOW COLUMNS FROM edges LIKE 'weight';")
        result = cursor.fetchone()
        if not result:
            cursor.execute("ALTER TABLE edges ADD COLUMN weight FLOAT DEFAULT NULL;")
            connection.commit()
            print("Weight column added successfully.")
            logging.info("Weight column added successfully.!")
        else:
            print("Weight column already exists.")
            logging.info("Weight column already exists.")
    finally:
        cursor.close()

# Function to update 5% of edges with random weights between 0 and 1
def update_edges(connection, num_iterations):
    """
    Updates a subset of edges in the 'edges' table with random weights and measures execution time.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.
        num_iterations (int): The number of times to perform the update operation.

    Returns:
        tuple: A tuple containing two values:
            - mean_time (float): Mean execution time for the update operation.
            - std_dev_time (float): Standard deviation of the execution time.
    """

    logging.info("Edge udate operation starting!")
    percentage = 5
    execution_times = []

    print(f"Assigning random float weights to {percentage}% of the edges.")

    # Ensure the 'weight' column exists before assigning weights
    add_weight_column(connection)

    cursor = connection.cursor()

    for _ in range(num_iterations):
        try:
            # Enable profiling to measure execution time
            cursor.execute("SET profiling = 1;")
            
            # Create a temporary table to store selected edge IDs
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_edge_ids (
                    source BIGINT,
                    target BIGINT,
                    PRIMARY KEY (source, target)
                );
            """)

            # Get the total number of edges
            cursor.execute("SELECT COUNT(*) FROM edges;")
            total_edges = cursor.fetchone()[0]

            # Calculate how many edges to update
            num_edges_to_update = int(total_edges * (percentage / 100))

            # Insert the selected edge pairs (source, target) into the temporary table
            cursor.execute(f"""
                INSERT IGNORE INTO temp_edge_ids (source, target)
                SELECT source, target
                FROM edges
                ORDER BY source, target  -- Sorting by source and target
                LIMIT {num_edges_to_update};
            """)

            # Perform the update operation
            cursor.execute("""
                UPDATE edges e
                JOIN temp_edge_ids t 
                ON e.source = t.source AND e.target = t.target
                SET e.weight = RAND();
            """)

            # Measure execution time
            cursor.execute("SHOW PROFILES;")
            profiles = cursor.fetchall()
            if profiles:
                # Calculate total time for all queries executed during this iteration
                total_time = sum(profile[1] for profile in profiles)
                execution_times.append(total_time)
            else:
                print("No profiling information available.")
        
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            connection.rollback()
        finally:
            # Drop the temporary table
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_edge_ids;")
            # Disable profiling
            cursor.execute("SET profiling = 0;")
            print("Edge update operation completed!")

    cursor.close()

    if execution_times:
        mean_time = np.mean(execution_times)
        std_dev_time = np.std(execution_times)
    else:
        mean_time = std_dev_time = None

    logging.info("Edge update operation completed!")

    return mean_time, std_dev_time

# Function to delete all data from tables in batches
def delete_all(connection, num_iterations, batch_size=30000):
    """
    Deletes all rows from the 'nodes', 'edges', and 'labels' tables, measuring the execution time.

    Parameters:
        connection (MySQL connection object): Connection to the MySQL database.
        num_iterations (int): The number of times to repeat the delete operation.

    Returns:
        tuple: A tuple containing two values:
            - mean_time (float): Mean execution time for the delete operation.
            - std_dev_time (float): Standard deviation of the execution time.
    """

    logging.info("Delete operation starting!")
    
    cursor = connection.cursor()
    execution_times = []

    try:
        for iteration in range(num_iterations):
            logging.info(f"Starting iteration {iteration + 1}/{num_iterations}")
            
            # Enable profiling to measure execution time
            cursor.execute("SET profiling = 1;")
            
            tables = ["nodes", "edges", "labels"]
            
            for table in tables:
                # Get the total number of rows in the current table
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                total_rows = cursor.fetchone()[0]
                logging.info(f"Deleting from {table} with {total_rows} rows")
                
                # Delete in batches
                for start in range(0, total_rows, batch_size):
                    cursor.execute(f"DELETE FROM {table} LIMIT {batch_size}")
                    connection.commit()
            
            # Get the execution time from profiling for this iteration
            cursor.execute("SHOW PROFILES;")
            profiles = cursor.fetchall()
            iteration_time = sum(profile[1] for profile in profiles)  # Summing the durations of all queries
            execution_times.append(iteration_time)
            logging.info(f"Iteration {iteration + 1} completed with execution time: {iteration_time:.3f} seconds")
            
            # Disable profiling after each iteration
            cursor.execute("SET profiling = 0;")
        
    finally:
        logging.info("Delete operation completed!")
        cursor.close()

    if execution_times:
        mean_time = np.mean(execution_times)
        std_dev_time = np.std(execution_times)
        return mean_time, std_dev_time
    else:
        return float('nan'), float('nan')  # Return NaN if no iterations were successful

# Main Function
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

    # Connect to MySQL server
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Berlin!321"
    )

    # Define file paths and database names based on files on the server
    
    edge_file = '/home/spandey/Master-Thesis/Code_Final/data/data/syn_data/edge_index_100000_nodes_10_edges.csv'
    node_file = '/home/spandey/Master-Thesis/Code_Final/data/data/syn_data/X_100000_nodes_10_edges.csv'
    label_file = '/home/spandey/Master-Thesis/Code_Final/data/data/syn_data/y_100000_nodes_10_edges.csv'

    num_iterations = 10
    num_hops = 3

    # Construct database name dynamically
    num_nodes, num_edges = extract_file_info(node_file)
    database_name = f"query_ops_{num_nodes}_nodes_{num_edges}_edges_{num_iterations}_iter"
    log_file_name = f"mysql_{num_nodes}N_{num_edges}E_{num_iterations}Iterations.log"
 
    # Set up logging with the dynamic log file name
    logger = logging.getLogger()
    if not logger.hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler(log_file_name, mode='w'),
                logging.StreamHandler(sys.stdout)
            ]
        )


    # Connect to MySQL
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Berlin!321"
    )

    # Create a new database dynamically if it does not exist
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.close()

    # Connect to the newly created database
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Berlin!321",
        database=database_name
    )

    # Create tables
    create_tables_with_index(connection)

    # Log results
    logging.info(f"Results for dataset_{num_nodes}_nodes_{num_edges}_edges for {num_iterations} iterations in MySQL are:")
    
    # Measure performance for uploading edges
    logging.info("Edges Upload Performance:")
   
    mean_time, std_dev_time, mean_space, std_space = measure_performance(connection, load_edges, num_iterations, edge_file)
    logging.info(f"Mean time: {mean_time:3f} seconds, Std Dev time: {std_dev_time:3f}")
    logging.info(f"Mean space: {mean_space:3f} MB, Std Dev space: {std_space:3f} MB")

    # Measure performance for uploading nodes
    logging.info("Nodes Upload Performance:")
    mean_time, std_dev_time, mean_space, std_space = measure_performance(connection, load_nodes, num_iterations, node_file)
    logging.info(f"Mean time: {mean_time:3f} seconds, Std Dev time: {std_dev_time:3f}")
    logging.info(f"Mean space: {mean_space:3f} MB, Std Dev space: {std_space:3f} MB")

    # Measure performance for uploading labels
    logging.info("Labels Upload Performance:")
    mean_time, std_dev_time, mean_space, std_space = measure_performance(connection, load_labels, num_iterations, label_file)
    logging.info(f"Mean time: {mean_time:3f} seconds, Std Dev time: {std_dev_time:3f}")
    logging.info(f"Mean space: {mean_space:3f} MB, Std Dev space: {std_space:3f} MB")

    # Data loading and index timing
    mean_data_loading_index_time, std_data_loading_index_time  = create_tables_with_index(connection)

    # Measure performance for read operation
    logging.info("Read Operation Performance:")
    mean_execution_time, std_dev_execution_time, mean_read_index_time, std_read_index_time = read(connection, num_hops, num_iterations)
    logging.info(f"Mean execution time for read: {mean_execution_time:3f} seconds, Std Dev execution time for read: {std_dev_execution_time:3f}")

    # Combine index times
    print("Index Creation Performance:")
    total_index_mean_time = mean_data_loading_index_time + mean_read_index_time    
    total_index_std_time = np.sqrt(std_data_loading_index_time**2 + std_read_index_time**2)
    logging.info(f"Total Mean Index Creation Time: {total_index_mean_time:3f}seconds, Total SD Index Creation Time: {total_index_std_time:3f}")
         
    # Perform update operation on nodes
    logging.info("Node Update Operation Performance:")
    mean_execution_time, std_dev_execution_time = update_nodes(connection, num_iterations)
    logging.info(f"Mean execution time: {mean_execution_time:3f} seconds, Std Dev execution time: {std_dev_execution_time:3f}")

    # Perform update operation on edges by adding weights
    logging.info("Edge Update Operation Performance:")
    mean_execution_time, std_dev_execution_time = update_edges(connection, num_iterations)
    logging.info(f"Mean execution time: {mean_execution_time:3f} seconds, Std Dev execution time: {std_dev_execution_time:3f}")

    # Perform delete operation
    logging.info("Delete Operation Performance:")
    mean_execution_time, std_dev_execution_time = delete_all(connection, num_iterations)
    logging.info(f"Mean execution time: {mean_execution_time:3f} seconds, Std Dev execution time: {std_dev_execution_time:3f}")

    # Close connection
    connection.close()

    logging.info("All operations completed successfully.")