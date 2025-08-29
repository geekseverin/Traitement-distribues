#!/bin/bash
# Créer ce fichier : applications/streaming-app/start-app.sh

echo "🚀 Starting Flask Streaming Application..."

cd /applications/streaming-app

# Install dependencies
pip3 install flask pyspark pymongo requests

# Set environment variables
export FLASK_APP=app.py
export FLASK_ENV=development

# Start Flask app
python3 app.py &

echo "✅ Flask app started on port 5000"
echo "🌐 Access dashboard at: http://localhost:5000"