#!/bin/bash

# Start SSH service
service ssh start

# Wait for namenode to be ready
echo "Waiting for namenode to be ready..."
while ! nc -z namenode 9000; do
    sleep 2
done

echo "Starting DataNode..."
$HADOOP_HOME/bin/hdfs datanode &

echo "Starting NodeManager..."
$HADOOP_HOME/bin/yarn nodemanager &

echo "Starting Spark Worker..."
$SPARK_HOME/sbin/start-worker.sh spark://namenode:7077

# Keep container running
echo "Slave services started. Keeping container alive..."
tail -f /dev/null