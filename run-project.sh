#!/bin/bash

# Complete project execution script
# This script will run the entire Big Data pipeline

echo "🚀 Starting Big Data Pipeline Execution..."

# Step 1: Start all services
echo "📋 Step 1: Starting Docker services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to initialize..."
sleep 60

# Step 2: Check if services are running
echo "📋 Step 2: Checking service status..."
docker-compose ps

# Step 3: Copy data to HDFS
echo "📋 Step 3: Loading data into HDFS..."
docker exec namenode bash -c "
    # Create directories in HDFS
    hdfs dfs -mkdir -p /data/input
    hdfs dfs -mkdir -p /data/output
    
    # Copy sample data to HDFS
    hdfs dfs -put /data/sample_data.csv /data/input/
    
    # Verify data is loaded
    hdfs dfs -ls /data/input/
"

# Step 4: Load data into MongoDB
echo "📋 Step 4: Loading data into MongoDB..."
docker exec mongodb mongoimport \
    --db bigdata \
    --collection employees \
    --type csv \
    --headerline \
    --file /docker-entrypoint-initdb.d/sample_employees.csv

# Step 5: Run Pig analysis
echo "📋 Step 5: Running Apache Pig analysis..."
docker exec namenode pig -f /scripts/pig/data-exploration.pig

# Step 6: Test MongoDB connection with Pig
echo "📋 Step 6: Testing MongoDB-Hadoop integration..."
docker exec namenode pig -f /scripts/pig/mongodb-connection.pig

# Step 7: Start streaming application
echo "📋 Step 7: Starting streaming application..."
docker exec -d namenode python3 /applications/streaming-app/app.py

# Step 8: Display results
echo "📋 Step 8: Displaying analysis results..."
echo ""
echo "=== HDFS Analysis Results ==="
docker exec namenode hdfs dfs -cat /data/output/department_analysis/part-r-00000

echo ""
echo "=== City Distribution Results ==="
docker exec namenode hdfs dfs -cat /data/output/city_distribution/part-r-00000

echo ""
echo "=== MongoDB Results ==="
docker exec mongodb mongo bigdata --eval "db.results.find().pretty()"

# Step 9: Show access URLs
echo ""
echo "✅ Pipeline execution completed!"
echo ""
echo "🌐 Access URLs:"
echo "- Hadoop NameNode UI: http://localhost:9870"
echo "- Spark Master UI: http://localhost:8080"
echo "- Streaming Dashboard: http://localhost:5000"
echo ""
echo "📊 To check results:"
echo "docker exec namenode hdfs dfs -ls /data/output/"
echo "docker exec mongodb mongo bigdata --eval 'db.employees.count()'"
echo ""
echo "🎬 For video demonstration, record your screen showing:"
echo "1. Docker services status (docker-compose ps)"
echo "2. Hadoop UI (http://localhost:9870)"
echo "3. HDFS file browser"
echo "4. Pig script execution"
echo "5. MongoDB data"
echo "6. Streaming dashboard"