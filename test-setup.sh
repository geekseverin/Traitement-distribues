#!/bin/bash

echo "🔧 Testing Big Data Setup..."

# Step 1: Check file structure
echo "📁 Checking file structure..."
echo "Hadoop Master Dockerfile: $(ls docker/hadoop-master/Dockerfile 2>/dev/null && echo '✅' || echo '❌')"
echo "Hadoop Slave Dockerfile: $(ls docker/hadoop-slave/Dockerfile 2>/dev/null && echo '✅' || echo '❌')"
echo "Docker Compose: $(ls docker-compose.yml 2>/dev/null && echo '✅' || echo '❌')"

# Step 2: Clean up
echo "🧹 Cleaning up..."
docker-compose down -v 2>/dev/null || true

# Step 3: Build only (no run)
echo "🔨 Testing build..."
docker-compose build --no-cache

if [ $? -eq 0 ]; then
    echo "✅ Build successful!"
    
    # Step 4: Start services
    echo "🚀 Starting services..."
    docker-compose up -d
    
    # Step 5: Wait and check
    echo "⏳ Waiting 60 seconds..."
    sleep 60
    
    echo "📋 Service status:"
    docker-compose ps
    
    # Step 6: Test namenode
    echo "🧪 Testing NameNode..."
    if docker exec namenode hdfs version 2>/dev/null; then
        echo "✅ NameNode is working!"
    else
        echo "❌ NameNode failed"
        docker logs namenode | tail -10
    fi
    
else
    echo "❌ Build failed!"
    exit 1
fi