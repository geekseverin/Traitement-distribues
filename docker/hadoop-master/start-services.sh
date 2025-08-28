#!/bin/bash

echo "🚀 Starting Hadoop Master Services..."

# Set proper environment
export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/opt/spark

# Add to PATH
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/logs /hadoop/temp

# Set permissions
chown -R root:root /hadoop

# Configure Hadoop environment
echo "⚙️  Configuring Hadoop environment..."
echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh
echo "export HADOOP_HOME=$HADOOP_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh

# Start SSH (required for Hadoop)
echo "🔐 Starting SSH service..."
service ssh start
sleep 2

# Format NameNode if needed
if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "💾 Formatting NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
    if [ $? -ne 0 ]; then
        echo "❌ NameNode format failed!"
        exit 1
    fi
    echo "✅ NameNode formatted successfully"
fi

# Start NameNode
echo "🎯 Starting NameNode..."
$HADOOP_HOME/bin/hdfs --daemon start namenode
sleep 10

# Check if NameNode started
if ! pgrep -f "org.apache.hadoop.hdfs.server.namenode.NameNode" > /dev/null; then
    echo "❌ NameNode failed to start!"
    echo "📜 Checking NameNode logs..."
    cat /opt/hadoop/logs/hadoop-*-namenode-*.log | tail -20
    exit 1
fi
echo "✅ NameNode started successfully"

# Start DataNode
echo "💽 Starting DataNode..."
$HADOOP_HOME/bin/hdfs --daemon start datanode
sleep 5

# Start YARN ResourceManager
echo "📊 Starting YARN ResourceManager..."
$HADOOP_HOME/bin/yarn --daemon start resourcemanager
sleep 5

# Start YARN NodeManager
echo "🔧 Starting YARN NodeManager..."
$HADOOP_HOME/bin/yarn --daemon start nodemanager
sleep 5

# Start Spark Master
echo "⚡ Starting Spark Master..."
$SPARK_HOME/sbin/start-master.sh
sleep 5

# Wait for services to be ready
echo "⏳ Waiting for services to initialize..."
sleep 20

# Create HDFS directories
echo "📂 Creating HDFS directories..."
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/root
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /data/input
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /data/output

# Load sample data if available
if [ -f "/data/raw/sample_data.csv" ]; then
    echo "📊 Loading sample data to HDFS..."
    $HADOOP_HOME/bin/hdfs dfs -put /data/raw/sample_data.csv /data/input/ 2>/dev/null || true
fi

# Install Python packages
echo "🐍 Installing Python packages..."
pip3 install flask pyspark pymongo requests 2>/dev/null || true

# Display status
echo ""
echo "✅ All services started successfully!"
echo "🌐 Web UIs:"
echo "  - NameNode: http://localhost:9870"
echo "  - Spark Master: http://localhost:8080"
echo ""

# Show running Java processes
echo "🔍 Running Hadoop processes:"
pgrep -f "hadoop" | wc -l | xargs echo "Hadoop processes:"
pgrep -f "spark" | wc -l | xargs echo "Spark processes:"

# Keep container running
echo "🔄 Keeping container alive..."
tail -f /opt/hadoop/logs/*.log 2>/dev/null &
wait