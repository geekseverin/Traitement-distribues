#!/bin/bash

echo "Starting DataNode slave services..."

# Set proper environment
export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/opt/spark

# Create necessary directories
mkdir -p /hadoop/dfs/data /hadoop/dfs/secondary /hadoop/logs /hadoop/temp

# Set permissions
chown -R root:root /hadoop

# Start SSH service
service ssh start

# Wait for namenode to be ready
echo "Waiting for namenode to be ready..."
while ! nc -z namenode 9000; do
    echo "Waiting for namenode connection..."
    sleep 5
done

echo "NameNode is ready, starting services..."

# Determine node type
if [ "$NODE_TYPE" = "secondary" ]; then
    echo "Starting Secondary NameNode..."
    $HADOOP_HOME/bin/hdfs --daemon start secondarynamenode
else
    echo "Starting DataNode..."
    $HADOOP_HOME/bin/hdfs --daemon start datanode
    sleep 5
    
    echo "Starting NodeManager..."
    $HADOOP_HOME/bin/yarn --daemon start nodemanager
    sleep 5
    
    echo "Starting Spark Worker..."
    $SPARK_HOME/sbin/start-worker.sh spark://namenode:7077
fi

# Keep container running
echo "Services started. Keeping container alive..."
tail -f /opt/hadoop/logs/*.log 2>/dev/null &
wait