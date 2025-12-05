"""
Script d'indexation Elasticsearch pour NetPlag
Indexe les résultats de plagiat depuis HDFS vers Elasticsearch
Permet la recherche et l'analyse des résultats de plagiat
"""

import os
import sys
from datetime import datetime

# Detect if running in Docker
IN_DOCKER = os.path.exists('/.dockerenv') or os.getenv('IN_DOCKER', 'false').lower() == 'true'

if not IN_DOCKER:
    # Only set these when running on Windows host
    os.environ['JAVA_HOME'] = r'D:\jdk17'
    os.environ['HADOOP_HOME'] = r'D:\hadoopbin\winutils\hadoop-3.0.1'
    os.environ['SPARK_HOME'] = r'D:\DOWNLOAD\spark-3.5.7-bin-hadoop3\spark-3.5.7-bin-hadoop3'
    
    java_bin = os.path.join(os.environ['JAVA_HOME'], 'bin')
    hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
    current_path = os.environ.get('PATH', '')
    os.environ['PATH'] = f"{java_bin};{hadoop_bin};{current_path}"
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import tempfile

# Import configurations
from config.hdfs_config import (
    HDFS_REPORTS, HDFS_PLAGIARISM_RESULTS, HDFS_STREAMING_VECTORS,
    get_spark_hdfs_config
)
from config.elasticsearch_config import (
    ES_URL, ES_INDEX_PLAGIARISM, ES_INDEX_DOCUMENTS, ES_INDEX_ANALYSIS,
    PLAGIARISM_INDEX_MAPPING, DOCUMENT_INDEX_MAPPING, ANALYSIS_INDEX_MAPPING
)

# Import Elasticsearch client
try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
except ImportError:
    print("[ERROR] Elasticsearch client not installed. Run: pip install elasticsearch>=8.11.0")
    sys.exit(1)

print("\n" + "="*60)
print("  NETPLAG - ELASTICSEARCH INDEXER")
print("="*60)
print(f"[INFO] Elasticsearch URL: {ES_URL}")
print(f"[INFO] Environnement: {'Docker' if IN_DOCKER else 'Windows Host'}")
print("="*60 + "\n")

# Initialize Elasticsearch client
print("[STEP 1/5] Connexion à Elasticsearch...")
try:
    # For Elasticsearch 8.x with security disabled
    es = Elasticsearch(
        [ES_URL],
        request_timeout=30
    )
    
    # Test connection by getting cluster info
    cluster_info = es.info()
    print(f"[OK] Connecté à Elasticsearch")
    print(f"     - Version: {cluster_info['version']['number']}")
    print(f"     - Cluster: {cluster_info['cluster_name']}")
    
except Exception as e:
    print(f"[ERROR] Impossible de se connecter à Elasticsearch: {e}")
    print(f"[ERROR] Vérifiez que Elasticsearch est démarré: docker-compose up -d elasticsearch")
    print(f"[ERROR] Testez la connexion: Invoke-WebRequest -Uri http://localhost:9200/_cluster/health")
    print(f"[ERROR] URL essayée: {ES_URL}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Create indices if they don't exist
print("\n[STEP 2/5] Création des indices Elasticsearch...")
indices_to_create = [
    (ES_INDEX_PLAGIARISM, PLAGIARISM_INDEX_MAPPING),
    (ES_INDEX_DOCUMENTS, DOCUMENT_INDEX_MAPPING),
    (ES_INDEX_ANALYSIS, ANALYSIS_INDEX_MAPPING)
]

for index_name, mapping in indices_to_create:
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=mapping)
            print(f"[OK] Indice créé: {index_name}")
        else:
            print(f"[INFO] Indice existe déjà: {index_name}")
    except Exception as e:
        print(f"[WARN] Erreur lors de la création de l'indice {index_name}: {e}")

# Initialize Spark
print("\n[STEP 3/5] Initialisation de Spark...")
temp_dir = tempfile.gettempdir()
spark_warehouse = os.path.join(temp_dir, "spark_warehouse")
spark_local = os.path.join(temp_dir, "spark_local")

spark_config = {
    "spark.app.name": "NetPlag_Elasticsearch_Indexer",
    "spark.master": "local[2]",
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
    "spark.sql.warehouse.dir": spark_warehouse,
    "spark.local.dir": spark_local
}
spark_config.update(get_spark_hdfs_config())

try:
    # Stop any existing Spark session
    try:
        spark_old = SparkSession.getActiveSession()
        if spark_old:
            spark_old.stop()
    except:
        pass
    
    spark_builder = SparkSession.builder
    for key, value in spark_config.items():
        spark_builder = spark_builder.config(key, value)
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[OK] Spark initialisé")
except Exception as e:
    print(f"[ERROR] Échec de l'initialisation Spark: {e}")
    sys.exit(1)

# Index plagiarism results from HDFS
print("\n[STEP 4/5] Indexation des résultats de plagiat depuis HDFS...")

def index_plagiarism_results():
    """Index plagiarism results from HDFS to Elasticsearch"""
    try:
        # Try to read from detailed_results first, then fallback to plagiarism_results
        sources = [
            f"{HDFS_REPORTS}/detailed_results",
            HDFS_PLAGIARISM_RESULTS
        ]
        
        df = None
        for source in sources:
            try:
                df = spark.read.parquet(source)
                count = df.count()
                if count > 0:
                    print(f"[OK] Données trouvées dans: {source}")
                    print(f"     - Nombre de résultats: {count:,}")
                    break
            except:
                continue
        
        if df is None or df.count() == 0:
            print("[WARN] Aucun résultat de plagiat trouvé dans HDFS")
            print("[INFO] Exécutez d'abord Step 2 (streaming) ou Step 3 (analysis)")
            return 0
        
        # Add timestamp if not present
        if "timestamp" not in df.columns:
            df = df.withColumn("timestamp", current_timestamp())
        
        # Convert to list of dictionaries for bulk indexing
        print("[INFO] Préparation des données pour l'indexation...")
        rows = df.collect()
        
        actions = []
        for row in rows:
            doc = {
                "_index": ES_INDEX_PLAGIARISM,
                "_source": {
                    "document_filename": row["document_filename"],
                    "reference_filename": row["reference_filename"],
                    "similarity_score": float(row["similarity_score"]),
                    "is_plagiarism": bool(row["is_plagiarism"]),
                    "timestamp": row["timestamp"].isoformat() if hasattr(row["timestamp"], "isoformat") else datetime.now().isoformat()
                }
            }
            actions.append(doc)
        
        # Bulk index
        print(f"[INFO] Indexation de {len(actions)} documents...")
        success, failed = bulk(es, actions, chunk_size=1000)
        
        print(f"[OK] Indexation terminée:")
        print(f"     - Documents indexés: {success}")
        if failed:
            print(f"     - Échecs: {len(failed)}")
        
        # Refresh index to make documents searchable
        es.indices.refresh(index=ES_INDEX_PLAGIARISM)
        
        return success
        
    except Exception as e:
        print(f"[ERROR] Erreur lors de l'indexation: {e}")
        import traceback
        traceback.print_exc()
        return 0

def index_analysis_summary():
    """Index analysis summary from HDFS to Elasticsearch"""
    try:
        summary_path = f"{HDFS_REPORTS}/summary"
        try:
            df = spark.read.parquet(summary_path)
            count = df.count()
            if count == 0:
                return 0
            
            print(f"[OK] Résumé d'analyse trouvé: {count} documents")
            
            # Add timestamp
            if "analysis_timestamp" not in df.columns:
                df = df.withColumn("analysis_timestamp", current_timestamp())
            
            rows = df.collect()
            actions = []
            for row in rows:
                doc = {
                    "_index": ES_INDEX_ANALYSIS,
                    "_source": {
                        "document_filename": row["document_filename"],
                        "num_matches": int(row["num_matches"]),
                        "max_score": float(row["max_score"]),
                        "avg_score": float(row["avg_score"]),
                        "analysis_timestamp": row["analysis_timestamp"].isoformat() if hasattr(row["analysis_timestamp"], "isoformat") else datetime.now().isoformat()
                    }
                }
                actions.append(doc)
            
            success, failed = bulk(es, actions, chunk_size=1000)
            print(f"[OK] Résumé indexé: {success} documents")
            
            es.indices.refresh(index=ES_INDEX_ANALYSIS)
            return success
            
        except Exception as e:
            print(f"[WARN] Impossible de lire le résumé: {e}")
            return 0
            
    except Exception as e:
        print(f"[ERROR] Erreur lors de l'indexation du résumé: {e}")
        return 0

# Index plagiarism results
plagiarism_count = index_plagiarism_results()

# Index analysis summary
summary_count = index_analysis_summary()

# Verify indexing
print("\n[STEP 5/5] Vérification de l'indexation...")
try:
    plagiarism_doc_count = es.count(index=ES_INDEX_PLAGIARISM)['count']
    analysis_doc_count = es.count(index=ES_INDEX_ANALYSIS)['count']
    
    print(f"[OK] Vérification terminée:")
    print(f"     - Documents dans {ES_INDEX_PLAGIARISM}: {plagiarism_doc_count:,}")
    print(f"     - Documents dans {ES_INDEX_ANALYSIS}: {analysis_doc_count:,}")
except Exception as e:
    print(f"[WARN] Erreur lors de la vérification: {e}")

print("\n" + "="*60)
print("  INDEXATION TERMINÉE AVEC SUCCÈS!")
print("="*60)
print(f"✓ Résultats de plagiat indexés: {plagiarism_count:,}")
print(f"✓ Résumés d'analyse indexés: {summary_count:,}")
print(f"\n✓ Elasticsearch est prêt pour la recherche!")
print(f"  - URL: {ES_URL}")
print(f"  - Indice plagiat: {ES_INDEX_PLAGIARISM}")
print(f"  - Indice analyse: {ES_INDEX_ANALYSIS}")
print("\nExemples de recherche:")
print(f"  - Tous les cas de plagiat: GET {ES_URL}/{ES_INDEX_PLAGIARISM}/_search")
print(f"  - Cas avec score > 0.8: GET {ES_URL}/{ES_INDEX_PLAGIARISM}/_search?q=similarity_score:>0.8")
print("="*60 + "\n")

spark.stop()
print("[OK] Session Spark fermée proprement")

