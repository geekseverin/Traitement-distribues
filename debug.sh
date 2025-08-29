#!/bin/bash

echo "🔍 Débogage des données HDFS..."

# Vérifier si HDFS fonctionne
echo "📊 Test HDFS:"
docker exec namenode hdfs dfs -ls /

echo ""
echo "📂 Contenu de /data:"
docker exec namenode hdfs dfs -ls /data/

echo ""
echo "📂 Contenu de /data/input:"
docker exec namenode hdfs dfs -ls /data/input/

echo ""
echo "📊 Contenu du fichier sample_data.csv:"
docker exec namenode hdfs dfs -cat /data/input/sample_data.csv | head -5

echo ""
echo "🔍 Vérification des permissions:"
docker exec namenode hdfs dfs -ls -la /data/input/

echo ""
echo "⚡ Test Spark simple:"
docker exec namenode spark-submit --version

echo ""
echo "🌐 Test de l'API Flask:"
curl -s http://localhost:5000/api/stats | head -100