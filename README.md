# sb-spark

Tasks:
1. Count film ratings
  - Scala, Map-Reduce
  - input: json
  - output: json
2. Content-based recommendation system
  - Spark Dataframes, TF-IDF
  - input: HDFS
  - output: json
3. Data Mart
  - sources:
    - Cassandra
    - Elasticsearch
    - HDFS
    - PostgreSQL
  - output: PostgreSQL table
4. Project
  - 1 
    - save logs from Kafka to HDFS (schedule)
  - 2
    - take logs from Kafka (Spark Streaming)
    - count agg functions
    - write to Kafka
  - 3
    - Spark ML (LogisticRegression)
  - 4
    - dashboards in Elastic Search: we check changes of class distribution over time
