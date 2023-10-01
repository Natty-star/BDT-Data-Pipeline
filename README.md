# BDT-Data-Pipeline

Big Data Technology: Using `Apache Kafka`, `Apache Zookeeper`, `Apache Spark`, `Apache Hive`, `Apache Hadoop` stream csv data into kafka and processed using spark, ingest to Hadoop

# Zookeeper

1. zookeeper
sh: sudo ./bin/zookeeper-server-start.sh config/zookeeper.properties

# Kafka

2. kafka
sh: ./bin/kafka-server-start.sh config/server.properties

# Hadoop

3. hadoop
pwd: /home/hadoop/hadoop/sbin
sh: ./start-all.sh
sh: ./stop-all.sh

# Hive

4. hive
   sh: hive --service metastore &
   sh: hive
   sh: hiveserver2

# Spark

5. spark
sh: ./sbin/start-all.sh
sh: ./sbin/stop-all.sh

# Derby
6. derby
sh: startNetworkServer
