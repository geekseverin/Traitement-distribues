#!/usr/bin/env python3
"""
Real Spark Streaming Application for Big Data Project
Using actual Spark jobs to process CSV data - FIXED VERSION
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
    'real_csv_records': 10,  # Fixé à 10 car vous avez 10 lignes
    'spark_status': 'Not Started'
}

class RealSparkProcessor:
    def __init__(self):
        self.spark = None
        self.original_data = None
        self.streaming_active = False
        self.data_processed_count = 0
        
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
            
            self.spark.sparkContext.setLogLevel("INFO")
            print("✅ Session Spark créée avec Master: spark://namenode:7077")
            return True
        except Exception as e:
            print(f"❌ Erreur Spark Session: {e}")
            return False
    
    def load_real_csv_data(self):
        """Charger les vraies données CSV avec Spark - UNIQUEMENT VOS 10 LIGNES"""
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
            
            # Calculer les statistiques réelles avec Spark - UNE SEULE FOIS
            total_count = df.count()
            avg_salary = df.agg(avg("salary")).collect()[0][0]
            
            # Groupement par département avec Spark
            dept_df = df.groupBy("department").count().orderBy("count", ascending=False)
            dept_results = dept_df.collect()
            dept_counts = {row["department"]: row["count"] for row in dept_results}
            
            # Mettre à jour les résultats FIXES
            streaming_results.update({
                'real_csv_records': total_count,
                'total_records': total_count,
                'avg_salary': round(avg_salary, 2) if avg_salary else 0,
                'department_counts': dept_counts,
                'last_update': time.strftime('%Y-%m-%d %H:%M:%S'),
                'spark_status': 'Data Loaded - Static Analysis',
                'spark_jobs_count': 1,
                'processing_rate': total_count
            })
            
            print(f"✅ Données chargées avec Spark: {total_count} enregistrements")
            print(f"📊 Départements trouvés: {list(dept_counts.keys())}")
            print(f"💰 Salaire moyen: {avg_salary:.2f}")
            
            return True
            
        except Exception as e:
            print(f"❌ Erreur chargement Spark: {e}")
            return False
    
    def start_real_spark_streaming(self):
        """Démarrer l'analyse statique des données réelles - PAS DE GÉNÉRATION"""
        global streaming_results
        
        if not self.original_data:
            print("❌ Pas de données originales")
            return
        
        self.streaming_active = True
        streaming_results['spark_status'] = 'Analyzing Real Data'
        job_counter = 1
        
        print("🔄 Démarrage de l'analyse Spark des données réelles...")
        
        # Analyse complète UNE SEULE FOIS
        try:
            print(f"📊 Analyse Spark Job #{job_counter} - VOS DONNÉES RÉELLES")
            
            # Traitement Spark distribué de VOS vraies données
            total_records = self.original_data.count()
            avg_salary_result = self.original_data.agg(avg("salary")).collect()[0][0]
            
            # Analyse par département pour VOS données
            dept_analysis = self.original_data.groupBy("department") \
                .agg(
                    count("*").alias("count"),
                    avg("salary").alias("avg_salary"),
                    min("salary").alias("min_salary"),
                    max("salary").alias("max_salary")
                ).collect()
            
            # Analyse par ville pour VOS données
            city_analysis = self.original_data.groupBy("city") \
                .agg(count("*").alias("count")) \
                .orderBy("count", ascending=False).collect()
            
            # Analyse par tranche d'âge pour VOS données
            age_analysis = self.original_data.withColumn(
                "age_group", 
                when(col("age") < 30, "Young")
                .when(col("age") < 50, "Middle")
                .otherwise("Senior")
            ).groupBy("age_group") \
             .agg(
                 count("*").alias("count"),
                 avg("salary").alias("avg_salary")
             ).collect()
            
            # Afficher les résultats réels
            print("📊 RÉSULTATS DE VOS VRAIES DONNÉES:")
            print(f"   Total employés: {total_records}")
            print(f"   Salaire moyen: {avg_salary_result:.2f}")
            
            print("📊 Analyse par département:")
            dept_counts = {}
            for row in dept_analysis:
                dept_name = row["department"]
                dept_count = row["count"]
                dept_avg_salary = row["avg_salary"]
                dept_counts[dept_name] = dept_count
                print(f"   {dept_name}: {dept_count} employés, salaire moyen: {dept_avg_salary:.2f}")
            
            print("📊 Analyse par ville:")
            for row in city_analysis:
                print(f"   {row['city']}: {row['count']} employés")
                
            print("📊 Analyse par tranche d'âge:")
            for row in age_analysis:
                print(f"   {row['age_group']}: {row['count']} employés, salaire moyen: {row['avg_salary']:.2f}")
            
            # Mettre à jour les statistiques FINALES avec VOS vraies données
            streaming_results.update({
                'total_records': total_records,
                'avg_salary': round(avg_salary_result, 2) if avg_salary_result else 0,
                'processing_rate': total_records,
                'spark_jobs_count': job_counter,
                'last_update': time.strftime('%Y-%m-%d %H:%M:%S'),
                'department_counts': dept_counts,
                'spark_status': f'Analysis Complete - {total_records} Real Records Processed'
            })
            
            print(f"✅ Analyse terminée: {total_records} vrais enregistrements traités")
            
            # Arrêter le streaming car l'analyse est terminée
            self.streaming_active = False
            streaming_results['spark_status'] = 'Analysis Complete - Real Data Only'
            
        except Exception as e:
            print(f"❌ Erreur Spark Streaming: {e}")
            self.streaming_active = False
            streaming_results['spark_status'] = 'Analysis Failed'
    
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
                    'executor_count': len(self.spark.sparkContext.statusTracker().getExecutorInfos()) - 1,
                    'data_source': 'Real CSV Data (10 records)',
                    'analysis_type': 'Static Analysis of Real Data'
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
            # Démarrer l'analyse Spark en arrière-plan
            thread = threading.Thread(target=spark_processor.start_real_spark_streaming, daemon=True)
            thread.start()
            
            return jsonify({
                'status': 'started',
                'message': f'Analyse Spark démarrée avec {streaming_results["real_csv_records"]} vrais enregistrements',
                'spark_master': 'spark://namenode:7077',
                'data_source': 'HDFS - Vos vraies données CSV (10 lignes)',
                'analysis_type': 'Static Analysis - No Data Generation'
            })
        else:
            return jsonify({'status': 'error', 'message': 'Échec chargement données Spark'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Erreur Spark: {str(e)}'})

@app.route('/api/stop')
def stop_processing():
    """Arrêter le streaming"""
    spark_processor.stop_streaming()
    return jsonify({'status': 'stopped', 'message': 'Analyse Spark arrêtée'})

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
    """Afficher TOUTES vos vraies données CSV"""
    try:
        if spark_processor.original_data:
            # Récupérer TOUTES vos données (pas seulement un échantillon)
            all_data = spark_processor.original_data.collect()
            result = [row.asDict() for row in all_data]
            return jsonify({
                'status': 'success',
                'total_records': streaming_results['real_csv_records'],
                'all_data': result,  # Toutes vos données
                'message': f'Voici vos {len(result)} vrais enregistrements'
            })
        else:
            return jsonify({'error': 'Pas de données Spark chargées'})
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    print("🚀 Démarrage Application Spark - ANALYSE DE VOS VRAIES DONNÉES UNIQUEMENT")
    print("⚡ Connexion à Spark Master: spark://namenode:7077")
    print("📊 Mode: Analyse statique de vos 10 lignes CSV réelles")
    print("🚫 Aucune génération de données aléatoires")
    
    # Démarrer Flask
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)