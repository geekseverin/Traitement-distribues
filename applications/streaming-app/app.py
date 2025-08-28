#!/usr/bin/env python3
"""
Dynamic Streaming Application for Big Data Project
Real-time data processing using Spark Streaming
"""

from flask import Flask, render_template, jsonify
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
import threading
import time
import json
import random

app = Flask(__name__)

# Global variables to store streaming results
streaming_results = {
    'total_records': 0,
    'avg_salary': 0,
    'department_counts': {},
    'last_update': None
}

class SparkStreamingApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("BigDataStreaming") \
            .master("spark://namenode:7077") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
    
    def start_streaming(self):
        """Start Spark Streaming job"""
        global streaming_results
        
        # Create streaming context
        ssc = StreamingContext(self.spark.sparkContext, 10)  # 10 second batches
        
        # For demo purposes, we'll generate sample streaming data
        def generate_sample_data():
            departments = ['IT', 'Sales', 'HR', 'Finance', 'Marketing']
            names = ['John', 'Jane', 'Bob', 'Alice', 'Charlie']
            
            while True:
                data = {
                    'id': random.randint(1, 1000),
                    'name': random.choice(names),
                    'department': random.choice(departments),
                    'salary': random.randint(30000, 100000),
                    'timestamp': int(time.time())
                }
                yield json.dumps(data)
                time.sleep(1)
        
        # Process streaming data
        def process_batch(rdd):
            global streaming_results
            
            if not rdd.isEmpty():
                # Convert RDD to DataFrame
                df = self.spark.read.json(rdd)
                
                # Update statistics
                total_count = df.count()
                avg_salary = df.agg(avg('salary')).collect()[0][0]
                dept_counts = df.groupBy('department').count().collect()
                
                # Update global results
                streaming_results['total_records'] += total_count
                streaming_results['avg_salary'] = round(avg_salary, 2) if avg_salary else 0
                streaming_results['department_counts'] = {row['department']: row['count'] for row in dept_counts}
                streaming_results['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"Processed {total_count} records, Avg Salary: {avg_salary}")
        
        # Create DStream from sample data
        data_stream = ssc.queueStream([])  # Simplified for demo
        data_stream.foreachRDD(process_batch)
        
        ssc.start()
        return ssc

# Flask Routes
@app.route('/')
def dashboard():
    """Main dashboard"""
    return render_template('dashboard.html')

@app.route('/api/stats')
def get_stats():
    """API endpoint to get current streaming statistics"""
    return jsonify(streaming_results)

@app.route('/api/start')
def start_processing():
    """Start the streaming processing"""
    try:
        streaming_app = SparkStreamingApp()
        ssc = streaming_app.start_streaming()
        return jsonify({'status': 'started', 'message': 'Streaming processing started'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

# Background data generator for demo
def background_data_generator():
    """Generate sample data in background"""
    global streaming_results
    departments = ['IT', 'Sales', 'HR', 'Finance', 'Marketing']
    
    while True:
        # Simulate incoming data
        streaming_results['total_records'] += random.randint(1, 5)
        streaming_results['avg_salary'] = random.randint(40000, 80000)
        streaming_results['department_counts'] = {
            dept: random.randint(1, 20) for dept in departments
        }
        streaming_results['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
        
        time.sleep(5)  # Update every 5 seconds

if __name__ == '__main__':
    # Start background data generation
    thread = threading.Thread(target=background_data_generator, daemon=True)
    thread.start()
    
    # Start Flask app
    app.run(host='0.0.0.0', port=5000, debug=True)