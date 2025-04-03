# Pyspark Microbatch Stream from Elasticsearch to Databricks Hive Tables
There are 2 notebooks in this repo to demonstrate doing Microbatch Streaming from Elasticsearch to Databricks Hive Tables.
- __Pyspark_Microbatch_Stream_from_Elasticsearch.ipynb__
  - this is the actual script to read data from elasticsearch and write to databricks continously, note that to test on your own data as a job you will need to remove a line of code.
- **Test_&_Verify_Pyspark_Microbatch_Stream.ipynb**
  - this is meant for testing and verifying data integrity from execution of Pyspark_Microbatch_Stream_from_Elasticsearch.ipynb using static mocked data

## Pyspark_Microbatch_Stream_from_Elasticsearch.ipynb
PySpark script that demonstrates a data ingestion pipeline reading data from Elasticsearch, processes it, and writes it to Delta tables.

The script can be scheduled as a job in databricks. 

__Note:__
To run the script continuously with your own realtime data, this line of code needs to be removed `current_execution_time = es_df.agg(max("update_timestamp")).collect()[0][0].isoformat()` it has been marked with this comment `# to be removed when executing continuously`. This line of code is meant to be used along with `Test_&_Verify_Pyspark_Microbatch_Stream.ipynb` for testing/ verification purposes.

### Key Features of the Script
- Reading of nested objects from Elasticsearch Indices
- Incremental Data Loading : Only new or updated records (based on update_timestamp) are processed.
- Dynamic Delta Table Management : Delta tables are dynamically created based on the _index field from Elasticsearch.
- Upsert Logic : Ensures data consistency by updating existing records and inserting new ones.
- Tracking Mechanism : Maintains a record of the last execution time to avoid reprocessing old data.

### Potential Improvements
- Error Handling : Add more robust error handling for Elasticsearch connectivity issues or schema mismatches.
- Logging : Introduce logging to track progress and debug issues.
- Performance Optimization : Use partitioning and bucketing in Delta tables for better performance with large datasets.

## Test_&_Verify_Pyspark_Microbatch_Stream.ipynb
This notebook is designed to test `Pyspark_Microbatch_Stream_from_Elasticsearch.ipynb` by analyzing trade transactions data stored in both Elasticsearch and Hive Metastore after each execution of `Pyspark_Microbatch_Stream_from_Elasticsearch.ipynb`.

Script creates sample data for initial load and 1 subsequent simulation for creation of new documents and update of existing documents in elasticsearch indices. 

### Prerequisites
- Elasticsearch: Ensure that Elasticsearch is running and accessible.
- Databricks Secrets: elasticsearch cloud url, username and password
- Hive Metastore: Ensure that Hive Metastore is configured and accessible.
- Databricks Cluster: The notebook is designed to run on a Databricks cluster with Spark version 15.4.x-scala2.12.

### Usage
Follow instructions in notebook to
- setup environment
- clear data in elasticsearch & hive tables
- load initial dataset into elasticsearch
- execution of `Pyspark_Microbatch_Stream_from_Elasticsearch.ipynb`
- simulate creation & update of documents in elasticsearch indices
- 2nd execution of `Pyspark_Microbatch_Stream_from_Elasticsearch.ipynb`
- verify data is synced in across both elasticsearch indices and hive tables
