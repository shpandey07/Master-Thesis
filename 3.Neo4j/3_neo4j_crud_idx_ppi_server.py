"""
Neo4j CRUD Operations and Performance Benchmarking Script 

This script is designed to perform various CRUD (Create, Read, Update, Delete) operations on a Neo4j graph database 
and to measure the performance of these operations on real-world Protein-Protein Interaction Networks dataset in batches on the server. It automates the following steps:

1. Database Connection:
   - Establishes a connection to the Neo4j database using specified credentials.

2. Data Loading:
   - Loads node, edge, and label data from CSV files into the database using APOC procedures.
   - Measures and logs the performance (time and space) of data uploads for multiple iterations.

3. Index Creation:
   - Dynamically creates indices on node properties to optimize query performance.
   - Measures the execution time for index creation.

4. Performance Measurement:
   - Measures execution times for loading nodes, labels, and edges.
   - Calculates and logs the mean execution time and standard deviation for each operation.

5. Disk Space Calculation:
   - Calculates the disk space utilized by nodes and relationships based on the number of records.
   - Logs statistics regarding disk space usage to assess resource efficiency.

6. Performance Logging:
   - Logs the results of each operation, including mean execution time, standard deviation, and space used, 
     to an external log file for further analysis.

This script supports custom configurations, such as:
- File paths for data (node, edge, label CSV files)
- Number of iterations for each operation

Usage:
Before running, ensure the Neo4j database is running and accessible with the specified credentials. Execute the script from the command line 
or an IDE, providing any necessary file paths and configurations within the main block.

Dependencies:
- 'neo4j': For connecting to the Neo4j database.
- 'numpy': For numerical operations.
- 're': For regular expression operations.
- 'time': For performance timing.
"""


# Import required libraries
import time
import logging
import numpy as np
from neo4j.exceptions import ServiceUnavailable
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from neo4j.exceptions import TransientError
from neo4j.exceptions import ServiceUnavailable
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Setup logging configuration (console + file)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("neo4j_operations.log"),
        logging.StreamHandler()
    ]
)

# Function to extract execution time from Neo4j query summary
def extract_execution_time_from_summary(summary):
    """
    Extracts the execution time from a Neo4j query summary.

    Args:
        summary (object): The summary object returned from a Neo4j query.

    Returns:
        float: The execution time in seconds.
    """

    return summary.result_consumed_after / 1000.0  # Convert milliseconds to seconds

# Function to calculate disk space
def calculate_disk_space(num_nodes, num_relationships):
    """
    Calculates the disk space used by nodes and relationships in Neo4j.

    Args:
        num_nodes (int): The number of nodes in the database.
        num_relationships (int): The number of relationships in the database.

    Returns:
        tuple: A tuple containing the disk space used by nodes and relationships in MB.
    """

    node_size_bytes = 14
    relationship_size_bytes = 33
    property_size_bytes = 41
    num_properties = 100

    node_space = ((num_nodes * node_size_bytes) + (num_nodes * num_properties * property_size_bytes)) / (1024 * 1024)
    relationship_space = (num_relationships * relationship_size_bytes) / (1024 * 1024)

    return node_space, relationship_space

# Fucntion to connect to Neo4j with many retries
def run_with_retries(uri, user, password="Bonn!321", retries=3, delay=5):
    """
    Connects to a Neo4j database with retry logic.

    Attempts to establish a connection to the specified Neo4j URI using the provided username and password. If the connection fails, it retries a specified number of times with a delay.

    Parameters:
    - `uri`: Neo4j database URI.
    - `user`: Username for authentication.
    - `password`: Password for authentication (default is "Bonn!321").
    - `retries`: Number of retry attempts (default is 3).
    - `delay`: Delay between retries in seconds (default is 5).

    Returns:
    - A Neo4j driver instance if successful.

    Raises:
    - `ServiceUnavailable`: If connection fails after all retries.
    """

    
    for attempt in range(retries):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password),
                                          connection_timeout=60,  # Increase timeout
                                          max_connection_pool_size=50)
            return driver  # Return driver on successful connection
        except ServiceUnavailable:
            print(f"Connection failed. Retry {attempt+1}/{retries}...")
            time.sleep(delay)
    raise ServiceUnavailable("Failed to connect to Neo4j after several retries.")


# Load data into Neo4j
def load_data(driver, node_file, label_file, edge_file, num_iterations):
    """
    Loads node, label, and edge data into Neo4j and measures performance.

    Args:
        driver (GraphDatabase.driver): The Neo4j driver instance for the connection.
        node_file (str): Path to the CSV file containing node data.
        label_file (str): Path to the CSV file containing label data.
        edge_file (str): Path to the CSV file containing edge data.
        num_iterations (int): The number of iterations for loading data.

    Returns:
        tuple: Statistics including mean and standard deviation of load times and disk space used.
    """

    logging.info("Load data operation starting!")

    # Initialize lists to store results for each iteration
    node_times = []
    label_times = []
    edge_times = []
    node_spaces = []
    relationship_spaces = []
    index_times = []

    def load_schema():
        with driver.session() as session:
            try:
                # Measure the time taken to create the index
                start_index_time = time.time()
                session.run("CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.idx)")
                end_index_time = time.time()
                index_creation_time = end_index_time - start_index_time
                index_times.append(index_creation_time)  # Store index creation time
            except Exception as e:
                print(f"Error during schema setup: {e}")

    def load_iteration():
        with driver.session() as session:
            # Load nodes (map dynamically extracts the columns for features)
            node_query = """
            CALL apoc.periodic.iterate(
                'CALL apoc.load.csv($file, {header:true}) YIELD map, lineNo RETURN map, lineNo',
                'CREATE (n:Node {idx: lineNo}) SET n += apoc.map.submap(map, keys(map))',
                {batchSize:30000, parallel:true, iterateList:true, params:{file: $file}}
            )
            """
            node_summary = session.run(node_query, file=node_file).consume()
            node_time = extract_execution_time_from_summary(node_summary)
            node_count = node_summary.counters.nodes_created
            logging.info("Node loading completed!")

            # Load labels (assuming single-column label file)
            label_query = """
            CALL apoc.periodic.iterate(
                'CALL apoc.load.csv($file, {header:false}) YIELD map, lineNo RETURN map.`0` AS label, lineNo AS row_index',
                'MATCH (n:Node {idx: row_index}) CALL apoc.create.addLabels(n, [label]) YIELD node RETURN node',
                {batchSize:30000, iterateList:true, parallel:true, params:{file: $file}}
            )
            """
            label_summary = session.run(label_query, file=label_file).consume()
            label_time = extract_execution_time_from_summary(label_summary)
            logging.info("Label loading completed!")

            # Load edges (from source and target indices in the edge file)
            edge_query = """
            CALL apoc.periodic.iterate(
                'CALL apoc.load.csv($file, {header:true}) YIELD map RETURN map',
                'WITH map, toInteger(map["0"]) AS source_idx, toInteger(map["1"]) AS target_idx ' +
                'MATCH (source:Node {idx: source_idx}) ' +
                'MATCH (target:Node {idx: target_idx}) ' +
                'MERGE (source)-[:CONNECTED_TO]->(target)',
                {batchSize:30000, parallel:true, iterateList:true, params:{file: $file}}
            )
            """
            edge_summary = session.run(edge_query, file=edge_file).consume()
            edge_time = extract_execution_time_from_summary(edge_summary)
            logging.info("Edge loading completed!")

            # Calculate disk space
            edge_count_query = "MATCH ()-[r:CONNECTED_TO]->() RETURN COUNT(r) AS num_relationships"
            num_relationships = session.run(edge_count_query).single()["num_relationships"]

            node_space, relationship_space = calculate_disk_space(node_count, num_relationships)

            return node_time, label_time, edge_time, node_space, relationship_space

    # Perform schema setup only once
    load_schema()

    # Measure execution times for multiple iterations
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(load_iteration) for _ in range(num_iterations)]
        for future in as_completed(futures):
            result = future.result()
            node_time, label_time, edge_time, node_space, relationship_space = result
            node_times.append(node_time)
            label_times.append(label_time)
            edge_times.append(edge_time)
            node_spaces.append(node_space)
            relationship_spaces.append(relationship_space)

    logging.info("Load operation completed!")
    # Calculate mean and standard deviation for times and spaces
    return {
        "node": (np.mean(node_times), np.std(node_times)),
        "label": (np.mean(label_times), np.std(label_times)),
        "edge": (np.mean(edge_times), np.std(edge_times)),
        "node_space": (np.mean(node_spaces), np.std(node_spaces)),
        "relationship_space": (np.mean(relationship_spaces), np.std(relationship_spaces)),
        "index_creation_time": (np.mean(index_times), np.std(index_times),)
    }


# Function to perform the read operation with edge index remapping and measure performance
def read(driver, num_hops, num_iterations):
    """
    Execute a read operation to retrieve subgraph data from a Neo4j database.

    This function creates temporary nodes and edges, performs hops 
    to explore the graph, and creates indexes. It measures execution 
    and index creation times across multiple iterations.

    Parameters:
        driver (object): Neo4j driver for executing queries.
        num_hops (int): Number of hops to traverse in the graph.
        num_iterations (int): Number of times to perform the read operation.

    Returns:
        tuple: Contains mean and standard deviation of execution times and index creation times:
            - mean_execution_time (float)
            - std_dev_execution_time (float)
            - mean_index_creation_time (float)
            - std_dev_index_creation_time (float)

    """
    print("Read operation starting!")
    execution_times = []
    index_creation_times = []  # List to store index creation times

    with driver.session() as session:
        for _ in range(num_iterations):
            total_execution_time = 0.0
            
            # Step 1: Create a temporary nodes collection
            create_temp_nodes_query = """
            MATCH (n:Node)
            WITH DISTINCT n
            RETURN id(n) AS node_id
            """
            start_time = time.time()
            temp_nodes = session.run(create_temp_nodes_query).data()
            total_execution_time += time.time() - start_time

            # Step 2: Create a remapping for new indices in Python
            remapping_dict = {node['node_id']: index for index, node in enumerate(temp_nodes)}

            # Step 3: Perform hops to get the desired neighborhood
            for depth in range(num_hops):
                hop_query = """
                MATCH (source:Node)-[:CONNECTED_TO]->(target:Node)
                WHERE id(source) IN $temp_nodes
                RETURN id(source) AS source_id, id(target) AS target_id
                """
                start_time = time.time()
                temp_nodes_ids = [item['node_id'] for item in temp_nodes]
                result = session.run(hop_query, temp_nodes=temp_nodes_ids).data()
                total_execution_time += time.time() - start_time
                
                # Add new nodes to temp_nodes for the next depth, ensuring no duplicates
                for edge in result:
                    source_id = edge['source_id']
                    target_id = edge['target_id']
                    
                    # Remap the source and target IDs
                    if source_id not in remapping_dict:  # Avoid duplicates
                        remapping_dict[source_id] = len(remapping_dict)  # New remapped ID
                        temp_nodes.append({'node_id': source_id})
                    
                    if target_id not in remapping_dict:  # Avoid duplicates
                        remapping_dict[target_id] = len(remapping_dict)  # New remapped ID
                        temp_nodes.append({'node_id': target_id})

            # Step 4: Create temporary edges using remapped indices
            temp_edges = []
            for edge in result:
                source_id = remapping_dict[edge['source_id']]
                target_id = remapping_dict[edge['target_id']]
                temp_edges.append((source_id, target_id))

            # Step 5:Create composite indexes on edges for better performance
            start_time = time.time()
            session.run("CREATE INDEX IF NOT EXISTS FOR ()-[r:CONNECTED_TO]-() ON (r.index)")
            index_creation_time = time.time() - start_time
            index_creation_times.append(index_creation_time)

            execution_times.append(total_execution_time)

    # Calculate mean and standard deviation of execution times
    mean_execution_time = np.mean(execution_times)
    std_dev_execution_time = np.std(execution_times)

    # Calculate mean and standard deviation of index creation times
    mean_index_creation_time = np.mean(index_creation_times)
    std_dev_index_creation_time = np.std(index_creation_times)

    print("Read operation completed!")
    return mean_execution_time, std_dev_execution_time, mean_index_creation_time, std_dev_index_creation_time


# Get number of nodes
def get_total_nodes(driver):
    """
    Retrieves the total number of nodes from the Neo4j database.

    Parameters:
    - `driver`: The Neo4j driver instance.

    Returns:
    - The total count of nodes labeled as `Node`.
    """

    with driver.session() as session:
        result = session.run("MATCH (n:Node) RETURN COUNT(n) AS total_nodes")
        total_nodes = result.single()["total_nodes"]
    return total_nodes

# Generate the dynamic update query using APOC
def generate_apoc_update_query(num_features):
    """
    Generates a dynamic APOC update query to set random feature values for nodes.

    Parameters:
    - `num_features`: The number of features to update for each node.

    Returns:
    - A string containing the APOC query for updating node features in batches.
    """

    # Generate the SET clause dynamically
    set_clause = ", ".join(f"n.feature_{i} = rand()" for i in range(num_features))
    return f"""
    CALL apoc.periodic.iterate(
        'MATCH (n:Node) WHERE id(n) >= $start_id AND id(n) < $end_id RETURN n',
        'SET {set_clause}',
        {{batchSize: 1000, parallel: true, params: {{start_id: $start_id, end_id: $end_id}}}}
    )
    """

# Update nodes in Neo4j in batches
def update_nodes(driver, num_iterations, batch_size=1000, num_features=100):
    """
    Updates nodes in Neo4j in batches with random feature values.

    Parameters:
    - `driver`: The Neo4j driver for database connection.
    - `num_iterations`: The number of iterations to perform the updates.
    - `batch_size`: The number of nodes to update per batch (default: 1000).
    - `num_features`: The number of random features to assign to each node (default: 100).

    Returns:
    - A tuple containing the mean execution time and standard deviation of the update operations.
    """

    logging.info("Node update operation starting!")

    # Function to perform a single update iteration
    def update_iteration(start_id, end_id):
        with driver.session() as session:
            # Query to update nodes
            update_query = generate_apoc_update_query(num_features)

            # Execute the query and measure the time using consume()
            result = session.run(update_query, start_id=start_id, end_id=end_id)
            summary = result.consume()  # Using consume() to get execution time

            # Convert execution time to seconds (from milliseconds)
            batch_time = summary.result_consumed_after / 1000.0

            return batch_time

    # Retrieve the total number of nodes dynamically
    total_nodes = get_total_nodes(driver)
    print(f"Total nodes: {total_nodes}")

    # Determine the range of IDs for each batch
    def get_id_ranges(total_nodes, batch_size):
        start_id = 0
        while start_id < total_nodes:
            end_id = min(start_id + batch_size, total_nodes)
            yield start_id, end_id
            start_id = end_id

    # List to store execution times for all batches
    execution_times = []

    # Use ThreadPoolExecutor to parallelize the update process
    with ThreadPoolExecutor() as executor:
        # Submit tasks for each ID range
        futures = [executor.submit(update_iteration, start_id, end_id) for start_id, end_id in get_id_ranges(total_nodes, batch_size)]

        # Collect the execution times from completed futures
        for future in as_completed(futures):
            execution_times.append(future.result())

    # Calculate the mean and standard deviation of execution times
    mean_update_time = np.mean(execution_times)
    std_dev_update_time = np.std(execution_times)

    logging.info("Node update operation completed!")
    return mean_update_time, std_dev_update_time


# Update edge weights in Neo4j using APOC
def update_edge_weights(driver, num_iterations, max_retries=5, batch_size_percent=0.5):
    """
    Updates edge weights in Neo4j using APOC for large datasets.

    Parameters:
    - `driver`: The Neo4j driver for database connection.
    - `num_iterations`: The number of times to perform the edge weight updates.
    - `max_retries`: Maximum number of retries for failed updates (default: 5).
    - `batch_size_percent`: Percentage of edges to update in each iteration (default: 0.5).

    Returns:
    - A tuple containing the mean execution time and standard deviation of the update operations,
    or (NaN, NaN) if all iterations fail.
    """

    logging.info("Update edge weights operation starting!")

    def update_iteration(retry_count=0):
        try:
            with driver.session() as session:
                # Calculate the batch size (e.g., 0.5% of total edges)
                batch_size = batch_size_percent / 100  # Convert percentage to fraction

                # Construct the APOC query to update 5% of the edges
                apoc_query = f"""
                CALL apoc.periodic.iterate(
                  'MATCH ()-[r:CONNECTED_TO]->() WHERE rand() < 0.05 RETURN r', 
                  'SET r.weight = rand()', 
                  {{batchSize: 10000, parallel: true}}
                ) YIELD batches, total 
                RETURN batches, total
                """

                # Execute the APOC query
                result = session.run(apoc_query)

                # Consume the result to ensure query execution and fetch summary
                summary = result.consume()

                # Calculate total time taken for this batch
                batch_time = summary.result_available_after / 1000.0  # Convert milliseconds to seconds

                print(f"Completed edge weight update iteration in {batch_time:.3f} seconds")
                return batch_time

        except (TransientError, ServiceUnavailable) as e:
            print(f"Error: {e}. Retrying ({retry_count}/{max_retries})...")
            if retry_count < max_retries:
                time.sleep(2 ** retry_count)  # Exponential backoff before retrying
                return update_iteration(retry_count + 1)
            else:
                print(f"Operation failed after {max_retries} retries.")
                return None

    # Measure execution times for multiple iterations
    execution_times = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(update_iteration) for _ in range(num_iterations)]
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                execution_times.append(result)
            else:
                print("Iteration failed. Skipping.")

    if execution_times:
        mean_update_time = np.mean(execution_times)
        std_dev_update_time = np.std(execution_times)
        logging.info("Edge update operation completed!")
        return mean_update_time, std_dev_update_time
    else:
        return float('nan'), float('nan')  # Return NaN if all iterations failed

# Delete all nodes and relationships from Neo4j
def delete(driver, num_iterations):
    """
    Delete all nodes and relationships in the Neo4j database and measure performance.

    This function executes multiple iterations of delete operations to remove all nodes and their associated relationships.

    Parameters:
    - driver: The Neo4j driver instance used to create sessions and execute queries.
    - num_iterations (int): The number of times to repeat the delete operations for performance measurement.

    Returns:
    - mean_time (float): The mean execution time of the delete operations over the iterations.
    - std_dev_time (float): The standard deviation of the execution times for the deletes.
    """

    logging.info("Delete operation starting!")

    # Query to delete relationships in batches using APOC
    delete_relationships_query = """
    CALL apoc.periodic.iterate(
      'MATCH ()-[r]->() RETURN r',
      'DELETE r',
      {batchSize: 30000, parallel: true}
    )
    YIELD batches, total, errorMessages
    RETURN batches, total, errorMessages
    """

    # Query to delete nodes in batches using APOC
    delete_nodes_query = """
    CALL apoc.periodic.iterate(
      'MATCH (n) RETURN n',
      'DELETE n',
      {batchSize: 30000, parallel: true}
    )
    YIELD batches, total, errorMessages
    RETURN batches, total, errorMessages
    """

    # Measure execution time for each iteration
    execution_times = []

    for iteration in range(num_iterations):
        # logging.info(f"Starting iteration {iteration + 1} of {num_iterations}")
        
        with driver.session() as session:
            # Delete relationships
            logging.info("Deleting relationships...")
            result = session.run(delete_relationships_query)
            summary = result.consume()
            delete_relationships_time = (summary.result_available_after + summary.result_consumed_after) / 1000.0
            execution_times.append(delete_relationships_time)
            logging.info(f"Relationships deleted in {delete_relationships_time:.3f} seconds")

            # Delete nodes
            logging.info("Deleting nodes...")
            result = session.run(delete_nodes_query)
            summary = result.consume()
            delete_nodes_time = (summary.result_available_after + summary.result_consumed_after) / 1000.0
            execution_times.append(delete_nodes_time)
            logging.info(f"Nodes deleted in {delete_nodes_time:.3f} seconds")

        # logging.info(f"Iteration {iteration + 1} completed!")

    # Calculate mean and standard deviation of execution times
    mean_time = np.mean(execution_times)
    std_dev_time = np.std(execution_times)

    logging.info(f"Delete operation completed after {num_iterations} iterations!")
    return mean_time, std_dev_time


# Main function to run all operations and log results
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
    # Credentials 
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "Berlin!321"

    # Parameters
    num_hops = 3
    num_iterations = 10

    edge_file = "syn_data/PPI_edge_index.csv"
    node_file = "syn_data/PPI_X.csv"
    label_file = "syn_data/PPI_y.csv"

    # Connect to the database using retry logic
    driver = run_with_retries(uri, username, password)

    dataset_name = f"dataset_PPI_{num_iterations}_iter"

    # Load data
    results = load_data(driver, node_file, label_file, edge_file, num_iterations)

    mean_node_time, std_node_time = results["node"]
    mean_label_time, std_label_time = results["label"]
    mean_edge_time, std_edge_time = results["edge"]
    mean_node_space, std_node_space = results["node_space"]
    mean_relationship_space, std_relationship_space = results["relationship_space"]
    mean_index_time, std_dev_read_index_time = results["index_creation_time"]

    # Print results
    logging.info(f"Results of CRUD operation on {dataset_name} in Neo4j (batch-wise) for {num_iterations} iterations are:")    

    # Measure performance for uploading edges
    logging.info("Edges Upload Performance:")
    logging.info(f"Mean time: {mean_edge_time:.3f} seconds, Std Dev time: {std_edge_time:.3f}")
    logging.info(f"Mean space: {mean_relationship_space:.3f} MB, Std Dev space: {std_relationship_space:.3f} MB")

    # Measure performance for uploading nodes
    logging.info("Nodes Upload Performance:")
    logging.info(f"Mean time: {mean_node_time:.3f} seconds, Std Dev time: {std_node_time:.3f}")
    logging.info(f"Mean space: {mean_node_space:.3f} MB, Std Dev space: {std_node_space:.3f} MB")

    # Measure performance for uploading labels
    logging.info("Labels Upload Performance:")
    logging.info(f"Mean time: {mean_label_time:.3f} seconds, Std Dev time: {std_label_time:.3f}")

    # Read data
    logging.info("Read Operation Performance:")
    mean_read_time, std_dev_read_time, mean_read_index_time, std_read_index_time  = read(driver, num_hops, num_iterations)
    logging.info(f"Mean read time: {mean_read_time:.3f}, Std dev: {std_dev_read_time:.3f}")

    # Combine index times
    logging.info("Index Creation Performance:")
    total_index_mean_time = mean_index_time + mean_read_index_time
    total_index_std_time = np.sqrt(std_dev_read_index_time**2 + std_read_index_time**2)
    logging.info(f"Mean index creation time: {total_index_mean_time:.3f} seconds, Std Dev index creation time: {total_index_std_time:.3f} seconds")    
    
    # Update nodes
    logging.info("Node Update Performance:")
    mean_update_time, std_dev_update_time = update_nodes(driver, num_iterations)
    logging.info(f"Mean update time: {mean_update_time:.3f}, Std dev: {std_dev_update_time:.3f}")

    # Update edge weights
    logging.info("Edge Update Performance:")
    mean_update_weights_time, std_dev_update_weights_time = update_edge_weights(driver, num_iterations)
    logging.info(f"Mean update weights time: {mean_update_weights_time:.3f}, Std dev: {std_dev_update_weights_time:.3f}")

    # Delete
    logging.info("Delete Operation Performance:")
    mean_delete_time, std_dev_delete_time = delete(driver, num_iterations)
    # mean_delete_time, std_dev_delete_time = delete(driver)
    logging.info(f"Mean delete time: {mean_delete_time:.3f}, Std dev: {std_dev_delete_time:.3f}")

    # Close connection
    driver.close()

    logging.info("All operations completed successfully.")