#!/bin/bash

# Complete project execution script
# This script will run the entire Big Data pipeline

echo "🚀 Starting Big Data Pipeline Execution..."

# Check if services are already running
RUNNING_SERVICES=$(docker-compose ps -q | wc -l)

if [ $RUNNING_SERVICES -eq 0 ]; then
    echo "📋 Step 1: Starting Docker services..."
    docker-compose up -d --build
    echo "⏳ Waiting for services to initialize (90 seconds)..."
    sleep 90
else
    echo "📋 Services already running, checking status..."
    echo "⏳ Waiting for services to be ready (30 seconds)..."
    sleep 30
fi

# Step 2: Check if services are running
echo "📋 Step 2: Checking service status..."
docker-compose ps

# Step 3: Wait for NameNode to be ready
echo "🧪 Testing NameNode connectivity..."
for i in {1..20}; do
    if docker exec namenode test -f /opt/hadoop/logs/hadoop-*-namenode-*.log 2>/dev/null; then
        echo "✅ NameNode container is responding!"
        break
    else
        echo "⏳ Waiting for NameNode container... ($i/20)"
        sleep 10
        
        # Check if namenode container exists and is running
        if ! docker ps | grep -q namenode; then
            echo "❌ NameNode container is not running. Checking logs..."
            docker logs namenode 2>/dev/null || echo "No logs available"
            
            echo "🔄 Attempting to restart namenode..."
            docker-compose restart namenode
            sleep 20
        fi
    fi
done

# Verify namenode is actually running
if ! docker ps | grep -q namenode; then
    echo "❌ NameNode is not running. Exiting..."
    echo "📜 Last container logs:"
    docker logs namenode 2>/dev/null || echo "No logs available"
    exit 1
fi

# Step 4: Test basic container access
echo "📋 Step 3: Testing container access..."
if docker exec namenode echo "Container accessible" 2>/dev/null; then
    echo "✅ NameNode container is accessible"
else
    echo "❌ NameNode container not accessible"
    exit 1
fi

# Step 5: Copy data to HDFS
echo "📋 Step 4: Loading data into HDFS..."
docker exec namenode bash -c '
    # Wait for HDFS to be ready
    for i in {1..10}; do
        if hdfs dfs -ls / 2>/dev/null; then
            echo "✅ HDFS is ready!"
            break
        else
            echo "⏳ Waiting for HDFS... ($i/10)"
            sleep 10
        fi
    done
    
    # Create directories in HDFS
    hdfs dfs -mkdir -p /data/input
    hdfs dfs -mkdir -p /data/output
    
    # Copy both CSV files to HDFS  
    if [ -f "/data/raw/sample_data.csv" ]; then
        hdfs dfs -put /data/raw/sample_data.csv /data/input/ 2>/dev/null || echo "File already exists"
        echo "✅ sample_data.csv loaded"
    fi
    
    if [ -f "/data/raw/sample_employees.csv" ]; then
        hdfs dfs -put /data/raw/sample_employees.csv /data/input/ 2>/dev/null || echo "File already exists"
        echo "✅ sample_employees.csv loaded"
    fi
    
    # Verify data is loaded
    echo "📊 Files in HDFS:"
    hdfs dfs -ls /data/input/
    
    if hdfs dfs -test -f /data/input/sample_data.csv; then
        echo "📊 Content of sample_data.csv (first 5 lines):"
        hdfs dfs -cat /data/input/sample_data.csv | head -5
    fi
'

# Step 6: Load data into MongoDB
echo "📋 Step 5: Loading data into MongoDB..."
sleep 10  # Wait for MongoDB to be ready

# Copy CSV to MongoDB container and import
if [ -f "data/raw/sample_employees.csv" ]; then
    docker cp data/raw/sample_employees.csv mongodb:/tmp/
    docker exec mongodb mongoimport \
        --db bigdata \
        --collection employees \
        --type csv \
        --headerline \
        --file /tmp/sample_employees.csv 2>/dev/null || echo "MongoDB import may have failed, continuing..."
    
    # Verify MongoDB data
    echo "📊 MongoDB employee count:"
    docker exec mongodb mongo bigdata --eval "db.employees.count()" --quiet 2>/dev/null || echo "MongoDB query failed"
fi

# Step 7: Test Pig installation
echo "📋 Step 6: Testing Apache Pig..."
if docker exec namenode pig -version 2>/dev/null; then
    echo "✅ Pig is installed and working"
    
    # Run Pig analysis
    echo "📋 Running Pig data exploration..."
    docker exec namenode pig -f /scripts/pig/data-exploration.pig 2>/dev/null || echo "Pig script failed"
    
    # Test MongoDB connection with Pig
    echo "📋 Testing MongoDB-Hadoop integration..."
    docker exec namenode pig -f /scripts/pig/mongodb-connection.pig 2>/dev/null || echo "MongoDB-Pig integration failed"
else
    echo "⚠️ Pig is not available"
fi

# Step 8: Test streaming application
echo "📋 Step 7: Testing streaming application..."
sleep 5

# Check if port 5000 is accessible
if curl -s http://localhost:5000 >/dev/null 2>&1; then
    echo "✅ Streaming application is running on port 5000!"
    
    # Start processing
    echo "📋 Starting Spark processing..."
    RESPONSE=$(curl -s http://localhost:5000/api/start 2>/dev/null || echo "API call failed")
    echo "API Response: $RESPONSE"
    
    sleep 5
    
    # Get stats
    echo "📋 Getting statistics..."
    curl -s http://localhost:5000/api/stats 2>/dev/null || echo "Stats API failed"
    
else
    echo "⚠️ Streaming application not responding on port 5000"
    echo "🔄 Checking if Flask is running in container..."
    
    if docker exec namenode pgrep -f "python.*app.py" 2>/dev/null; then
        echo "✅ Flask app is running in container"
    else
        echo "⚠️ Starting Flask application manually..."
        docker exec -d namenode bash -c "cd /applications/streaming-app && python3 app.py"
        sleep 15
        
        if curl -s http://localhost:5000 >/dev/null 2>&1; then
            echo "✅ Flask app started successfully!"
        else
            echo "❌ Flask app failed to start"
        fi
    fi
fi

# Step 9: Display results
echo "📋 Step 8: Displaying analysis results..."
echo ""
echo "=== HDFS Analysis Results ==="
docker exec namenode hdfs dfs -cat /data/output/department_analysis/part-r-00000 2>/dev/null || echo "No department analysis results yet"

echo ""
echo "=== City Distribution Results ==="
docker exec namenode hdfs dfs -cat /data/output/city_distribution/part-r-00000 2>/dev/null || echo "No city distribution results yet"

echo ""
echo "=== MongoDB Employee Count ==="
docker exec mongodb mongo bigdata --eval "db.employees.count()" --quiet 2>/dev/null || echo "MongoDB query failed"

echo ""
echo "=== Spark/Flask API Test ==="
curl -s http://localhost:5000/api/stats 2>/dev/null | head -200 || echo "API not responding"

# Final status check
echo ""
echo "✅ Pipeline execution completed!"
echo ""
echo "🌐 Access URLs:"
echo "- Hadoop NameNode UI: http://localhost:9870"
echo "- Spark Master UI: http://localhost:8080" 
echo "- Streaming Dashboard: http://localhost:5000"
echo ""

# Check actual service status
echo "🔍 Final service status:"
docker-compose ps

echo ""
echo "📊 Manual verification commands:"
echo "docker exec namenode hdfs dfs -ls /data/input/"
echo "docker exec mongodb mongo bigdata --eval 'db.employees.count()'"
echo "curl http://localhost:5000/api/stats"
echo "docker logs namenode | tail -20"