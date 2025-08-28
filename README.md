# Projet Big Data - Traitement Distribué
**Master 1 UCAO 2024-2025**

## 🎯 Objectif
Déployer un environnement Big Data complet avec Hadoop, Spark, MongoDB et Apache Pig pour l'analyse de données distribuée.

## 🏗️ Architecture du Système

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   NameNode      │    │  Secondary NN   │    │   DataNodes     │
│  (Master)       │    │                 │    │   (3 Slaves)    │
│                 │    │                 │    │                 │
│ - Hadoop Master │    │ - Backup        │    │ - Data Storage  │
│ - Spark Master  │    │ - Checkpoints   │    │ - Processing    │
│ - Pig Runtime   │    │                 │    │ - Spark Workers │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │    MongoDB      │
                    │   (Database)    │
                    │                 │
                    │ - Data Storage  │
                    │ - Collections   │
                    └─────────────────┘
```

## 🚀 Installation Rapide sur Ubuntu

### 1. Prérequis
```bash
# Cloner le projet
git clone <your-repo>
cd traitement-distribue-2024-2025

# Installer Docker et Docker Compose
chmod +x setup.sh
./setup.sh
```

### 2. Lancer l'environnement
```bash
# Démarrer tous les services
docker-compose up -d

# Vérifier que tous les services sont actifs
docker-compose ps
```

### 3. Exécuter le pipeline complet
```bash
chmod +x run-project.sh
./run-project.sh
```

## 📊 Composants du Projet

### 1. Cluster Hadoop (5 nœuds)
- **NameNode** : Nœud maître (namenode:9870)
- **Secondary NameNode** : Nœud secondaire (secondary-namenode:9868)
- **DataNodes** : 3 nœuds esclaves (datanode1-3:9864-9866)

### 2. Apache Spark
- **Master** : Coordination des jobs (namenode:8080)
- **Workers** : Traitement distribué sur les DataNodes

### 3. Apache Pig
- **Scripts d'analyse** : Exploration des données
- **Intégration MongoDB** : Lecture/écriture de données

### 4. MongoDB
- **Base de données** : Stockage NoSQL (mongodb:27017)
- **Collections** : employees, results

### 5. Application Dynamique
- **Dashboard Web** : Interface de monitoring (localhost:5000)
- **Streaming** : Traitement en temps réel avec Spark Streaming

## 📋 Étapes d'Exécution

### Étape 1: Analyse avec Apache Pig
```bash
# Exécuter l'analyse exploratoire
docker exec namenode pig -f /scripts/pig/data-exploration.pig

# Résultats disponibles dans HDFS
docker exec namenode hdfs dfs -ls /data/output/
```

### Étape 2: Intégration MongoDB
```bash
# Test de connexion MongoDB-Hadoop
docker exec namenode pig -f /scripts/pig/mongodb-connection.pig

# Vérifier les données MongoDB
docker exec mongodb mongo bigdata --eval "db.employees.find().pretty()"
```

### Étape 3: Application de Streaming
```bash
# Accéder au dashboard
open http://localhost:5000

# Démarrer le traitement en temps réel
curl http://localhost:5000/api/start
```

## 🔍 Interfaces Web

| Service | URL | Description |
|---------|-----|-------------|
| Hadoop NameNode | http://localhost:9870 | Interface HDFS |
| Spark Master | http://localhost:8080 | Cluster Spark |
| Dashboard Streaming | http://localhost:5000 | Application dynamique |

## 📈 Résultats Attendus

### 1. Analyse Pig - Statistiques par Département
```
IT,5,52600.0,41000.0,68000.0
Sales,3,56333.33,52000.0,61000.0
Finance,2,62500.0,57000.0,68000.0
```

### 2. Distribution par Ville
```
Paris,2
London,1
Tokyo,1
...
```

### 3. Groupes d'Âge
```
Young,8,45250.0
Middle,7,58571.43
Senior,5,61800.0
```

## 🎬 Vidéo de Démonstration

### Plan de la Vidéo (10-15 minutes)
1. **Introduction** (1 min)
   - Présentation du projet
   - Architecture du système

2. **Installation** (2-3 min)
   - Docker Compose up
   - Vérification des services

3. **Hadoop & HDFS** (2-3 min)
   - Interface web NameNode
   - Chargement des données
   - Navigation dans HDFS

4. **Apache Pig** (3-4 min)
   - Exécution des scripts
   - Analyse exploratoire
   - Résultats dans HDFS

5. **MongoDB Integration** (2-3 min)
   - Connexion Hadoop-MongoDB
   - Lecture des données
   - Stockage des résultats

6. **Application Dynamique** (2-3 min)
   - Dashboard web
   - Streaming en temps réel
   - Visualisations interactives

7. **Conclusion** (1 min)
   - Récapitulatif
   - Workflow final

## 📁 Structure des Fichiers

```
traitement-distribue-2024-2025/
├── docker-compose.yml              # Orchestration
├── docker/                         # Dockerfiles
├── scripts/pig/                     # Scripts Pig
├── applications/streaming-app/      # App dynamique
├── data/                           # Données d'exemple
├── setup.sh                       # Installation
├── run-project.sh                  # Exécution complète
└── README.md                       # Documentation
```

## 🛠️ Commandes Utiles

```bash
# Status des conteneurs
docker-compose ps

# Logs d'un service
docker-compose logs namenode

# Shell dans un conteneur
docker exec -it namenode bash

# Arrêter tous les services
docker-compose down

# Nettoyer complètement
docker-compose down -v --remove-orphans
```

## 🔧 Dépannage

### Problème : Services ne démarrent pas
```bash
# Vérifier les logs
docker-compose logs

# Redémarrer un service spécifique
docker-compose restart namenode
```

### Problème : Ports occupés
```bash
# Changer les ports dans docker-compose.yml
# Ou arrêter les services qui utilisent ces ports
sudo lsof -i :9870
```

## 📝 Livrables

✅ **Dockerfiles et scripts**
✅ **Scripts Pig pour l'analyse**  
✅ **Configuration Hadoop-MongoDB**
✅ **Application dynamique**
✅ **Diagrammes de workflow**
✅ **Vidéo de présentation**

## 👨‍💻 Auteur
**Étudiant Master 1 UCAO**  
**Année académique 2024-2025**

---

*Date limite de rendu : Vendredi 29 août 2025*