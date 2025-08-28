#!/usr/bin/env python3
"""
Real Spark Streaming Application for Big Data Project
Using actual Spark jobs to process CSV data
"""

from flask import Flask, render_template, jsonify
import os
import sys
import time
import threading
import json
import random
import subprocess

# Configuration Spark
os.environ['SPARK_HOME'] = '/opt/spark'
os.environ['PYTHONPATH'] = '/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip'
sys.path.append('/opt/spark/python')
sys.path.append('/opt/spark/python/lib/py4j-0.10.9.7-src.zip')

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    SPARK_AVAILABLE = True
    print("✅ Spark imports réussis")
except Exception as e:
    print(f"⚠️ Spark import échoué: {e}")
    SPARK_AVAILABLE = False

app = Flask(__name__)

# Variables globales pour les résultats
streaming_results = {
    'total_records': 0,
    'avg_salary': 0,
    'department_counts': {},
    'last_update': None,
    'processing_rate': 0,
    'spark_jobs_count': 0,
    'real_csv_records': 0,
    'spark_status': 'Not Started'
}

class RealSparkProcessor:
    def __init__(self):
        self.spark = None
        self.original_data = None
        self.streaming_active = False
        
    def initialize_spark(self):
        """Initialiser une vraie session Spark"""
        try:
            self.spark = SparkSession.builder \
                .appName("BigDataRealStreaming") \
                .master("spark://namenode:7077") \
                .config("spark.executor.memory", "1g") \
                .config("spark.driver.memory", "1g") \
                .config("spark.executor.cores", "2") \
                .config("spark.default.parallelism", "6") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("INFO")  # Pour voir les jobs
            print("✅ Session Spark créée avec Master: spark://namenode:7077")
            return True
        except Exception as e:
            print(f"❌ Erreur Spark Session: {e}")
            return False
    
    def load_real_csv_data(self):
        """Charger les vraies données CSV avec Spark"""
        global streaming_results
        
        if not self.spark:
            if not self.initialize_spark():
                return False
        
        try:
            # Définir le schéma exact de vos données
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True), 
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
                StructField("salary", DoubleType(), True),
                StructField("department", StringType(), True)
            ])
            
            # Lire depuis HDFS avec Spark
            hdfs_path = "hdfs://namenode:9000/data/input/sample_data.csv"
            
            df = self.spark.read \
                .option("header", "false") \
                .schema(schema) \
                .csv(hdfs_path)
            
            # Cacher les données pour réutilisation
            df.cache()
            self.original_data = df
            
            # Calculer les statistiques réelles avec Spark
            total_count = df.count()
            avg_salary = df.agg(avg("salary")).collect()[0][0]
            
            # Groupement par département avec Spark
            dept_df = df.groupBy("department").count().orderBy("count", ascending=False)
            dept_results = dept_df.collect()
            dept_counts = {row["department"]: row["count"] for row in dept_results}
            
            # Mettre à jour les résultats
            streaming_results.update({
                'real_csv_records': total_count,
                'total_records': total_count,
                'avg_salary': round(avg_salary, 2) if avg_salary else 0,
                'department_counts': dept_counts,
                'last_update': time.strftime('%Y-%m-%d %H:%M:%S'),
                'spark_status': 'Data Loaded',
                'spark_jobs_count': 1
            })
            
            print(f"✅ Données chargées avec Spark: {total_count} enregistrements")
            print(f"📊 Départements trouvés: {list(dept_counts.keys())}")
            
            return True
            
        except Exception as e:
            print(f"❌ Erreur chargement Spark: {e}")
            return False
    
    def start_real_spark_streaming(self):
        """Démarrer le vrai streaming Spark"""
        global streaming_results
        
        if not self.original_data:
            print("❌ Pas de données originales")
            return
        
        self.streaming_active = True
        streaming_results['spark_status'] = 'Streaming Active'
        job_counter = 1
        
        print("🔄 Démarrage du Spark Streaming réel...")
        
        while self.streaming_active:
            try:
                # Créer des micro-batches en échantillonnant les données
                sample_fraction = random.uniform(0.3, 0.8)
                batch_df = self.original_data.sample(sample_fraction, seed=int(time.time()))
                
                print(f"📊 Traitement Spark Job #{job_counter}")
                
                # Traitement Spark distribué
                batch_count = batch_df.count()
                batch_avg_salary = batch_df.agg(avg("salary")).collect()[0][0]
                
                # Analyse par département pour ce batch
                dept_analysis = batch_df.groupBy("department") \
                    .agg(
                        count("*").alias("count"),
                        avg("salary").alias("avg_salary"),
                        min("salary").alias("min_salary"),
                        max("salary").alias("max_salary")
                    ).collect()
                
                # Analyse par tranche d'âge
                age_analysis = batch_df.withColumn(
                    "age_group", 
                    when(col("age") < 30, "Young")
                    .when(col("age") < 50, "Middle")
                    .otherwise("Senior")
                ).groupBy("age_group").count().collect()
                
                # Mettre à jour les statistiques globales
                streaming_results['total_records'] += batch_count
                streaming_results['avg_salary'] = round(batch_avg_salary, 2) if batch_avg_salary else 0
                streaming_results['processing_rate'] = batch_count
                streaming_results['spark_jobs_count'] = job_counter
                streaming_results['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
                
                # Mettre à jour les départements avec les vrais résultats Spark
                new_dept_counts = {}
                for row in dept_analysis:
                    new_dept_counts[row["department"]] = row["count"]
                streaming_results['department_counts'] = new_dept_counts
                
                print(f"✅ Job #{job_counter} terminé: {batch_count} records, avg salary: {batch_avg_salary:.2f}")
                print(f"📈 Total traité: {streaming_results['total_records']} records")
                
                job_counter += 1
                time.sleep(10)  # Intervalle entre les jobs
                
            except Exception as e:
                print(f"❌ Erreur Spark Streaming: {e}")
                time.sleep(15)
    
    def stop_streaming(self):
        """Arrêter le streaming"""
        self.streaming_active = False
        streaming_results['spark_status'] = 'Stopped'
    
    def get_spark_application_info(self):
        """Récupérer les infos sur les applications Spark"""
        try:
            if self.spark:
                app_id = self.spark.sparkContext.applicationId
                app_name = self.spark.sparkContext.appName
                return {
                    'application_id': app_id,
                    'application_name': app_name,
                    'master_url': 'spark://namenode:7077',
                    'executor_count': len(self.spark.sparkContext.statusTracker().getExecutorInfos()) - 1
                }
        except:
            pass
        return None

# Instance globale
spark_processor = RealSparkProcessor()

@app.route('/')
def dashboard():
    """Dashboard principal"""
    return render_template('dashboard.html')

@app.route('/api/stats')
def get_stats():
    """API pour récupérer les statistiques"""
    # Ajouter les infos Spark
    spark_info = spark_processor.get_spark_application_info()
    result = streaming_results.copy()
    if spark_info:
        result['spark_application'] = spark_info
    return jsonify(result)

@app.route('/api/start')
def start_processing():
    """Démarrer le traitement Spark réel"""
    try:
        # Charger les données avec Spark
        if spark_processor.load_real_csv_data():
            # Démarrer le streaming Spark en arrière-plan
            thread = threading.Thread(target=spark_processor.start_real_spark_streaming, daemon=True)
            thread.start()
            
            return jsonify({
                'status': 'started',
                'message': f'Spark Streaming démarré avec {streaming_results["real_csv_records"]} records réels',
                'spark_master': 'spark://namenode:7077',
                'data_source': 'HDFS via Spark'
            })
        else:
            return jsonify({'status': 'error', 'message': 'Échec chargement données Spark'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Erreur Spark: {str(e)}'})

@app.route('/api/stop')
def stop_processing():
    """Arrêter le streaming"""
    spark_processor.stop_streaming()
    return jsonify({'status': 'stopped', 'message': 'Spark Streaming arrêté'})

@app.route('/api/spark-info')
def get_spark_info():
    """Informations détaillées sur Spark"""
    info = spark_processor.get_spark_application_info()
    if info:
        return jsonify(info)
    else:
        return jsonify({'error': 'Pas de session Spark active'})

@app.route('/api/real-data')
def get_real_data():
    """Afficher un échantillon des vraies données CSV"""
    try:
        if spark_processor.original_data:
            sample_data = spark_processor.original_data.limit(10).collect()
            result = [row.asDict() for row in sample_data]
            return jsonify({
                'status': 'success',
                'total_records': streaming_results['real_csv_records'],
                'sample_data': result
            })
        else:
            return jsonify({'error': 'Pas de données Spark chargées'})
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    print("🚀 Démarrage Application Spark Streaming RÉELLE")
    print("⚡ Connexion à Spark Master: spark://namenode:7077")
    
    # Démarrer Flask
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)