# Master-Thesis-Code
This repository includes the code for Master-Thesis on 'Comparative Analysis of Database Performance in Batch-Wise Querying of Large Graphs for Machine Learning'

# Master-Thesis Topic

## Overview
**Master thesis concept- Database evaluation**

**Abstract:**

In many domains, like biology, data is highly interconnected (e.g., interacting proteins). **Graphs** are **data structures** to **store** and **analyze** such **interconnected** **data**.  They consist of nodes that are connected by edges. Sometimes graphs are too large to fit into the memory of a device. Therefore, we can use external databases (e.g., Neo4j or MySQL) to **query** and **analyze** data **batch-wise**. The advertised **thesis aims** to **conduct** a **comprehensive comparative analysis of relational databases (MySQL and PostgreSQL), non-relational databases (MongoDB), and graph databases (Neo4j)** for **querying** data **batch-wise**. While the main focus of the study lies in the **optimization of read operations to query the data**, we would also like to **analyze all CRUD** (Create, Read, Update, and Delete) operations to get a holistic view of the databases‘ performances. The comparison should be applied to **synthetic datasets** with different-sized graphs and a **protein-protein interaction benchmark dataset.** By **comparing** MySQL, PostgreSQL, MongoDB, and Neo4j in a **controlled environment**, this research aims to furnish valuable **guidance** for **database selection** based on specific use cases and operational demands.

**Research Questions**

- Which database is most performant on CRUD operations (especially read operations) in terms of time and space requirements?
- How does introducing indexes change time and space requirements for CRUD operations?
- Which limitations exist when querying high-connected data (oversmoothing with low number of „hops“, high performance loss, high space requirements despite batching by querying complete graph with low number of „hops“)?
- (Is it possible to further increase the performance (e.g., parallelized access using multiple threads or denormalized tables)?)

When should we use which database (guidelines)?

**Database Management Systems**

- MySQL (relational database)
- PostgreSQL (relational database)
- MongoDB (document database)
- Neo4j (graph database)

**Dataset**

- Synthetic dataset with m (1,000; 100,000; 1,000,000) nodes (every node has n (5; 10; 20) edges) (Another test with 30,000 nodes and 10,000 edges per node)
- Protein-protein interaction dataset (OGB benchmark, PyG) – easy to understand, biological, connections are important (important for graph and JOIN evaluation), easy to access ([**https://ogb.stanford.edu/docs/nodeprop/**](https://ogb.stanford.edu/docs/nodeprop/)) (**https://pytorch-geometric.readthedocs.io/en/latest/generated/torch_geometric.datasets.PPI.html#torch_geometric.datasets.PPI**)

**Operations**

- Create (Upload data)
- Read (Batch-wise with different number of „hops“ for the respective number of layers in a GNN) – each with/without filter
    1. Read instances without any connections (Machine Learning without analyzing connections, e.g., Random Forest)
    2. Read instances with 1, 2 or 3 hops (Graph Neural Networks as Machine Learning algorithm)
    3. Read instances with 2 hops without specific instances (Additional filtering step)
- Update
- Delete
- Read with indexes [see 2) but with usage of smart indixes]
- (Read with parallelized accesses)

**Methodology**

- Python scripts with libraries as connectors
- Databases on database servers

Contact

- Sc. Daniel Walke - daniel.walke@ovgu.de
- Prof. Dr-Ing. Robert Heyer - robert.heyer@isas.de
- Ing. David Broneske - david.bronseke@ovgu.de
