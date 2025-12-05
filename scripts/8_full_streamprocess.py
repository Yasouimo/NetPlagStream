"""
NetPlag Full Stream Process
Complete streaming pipeline that:
1. Detects new files in HDFS stream_input
2. Processes files (TF-IDF, similarity detection) - Step 2
3. Runs batch analysis and generates reports - Step 3
4. Indexes results to Elasticsearch - Step 5

This is an all-in-one streaming solution that automatically processes,
analyzes, and indexes plagiarism detection results.
"""

import os
import sys
import tempfile
import subprocess
import signal
import atexit
from datetime import datetime

# Try to get short path (8.3 format) to avoid space issues
def get_short_path(long_path):
    """Get Windows 8.3 short path format to avoid space issues"""
    try:
        import win32api
        return win32api.GetShortPathName(long_path)
    except ImportError:
        try:
            result = subprocess.run(
                ['cmd', '/c', 'for', '%I', 'in', f'("{long_path}")', 'do', '@echo', '%~sI'],
                capture_output=True,
                text=True,
                shell=True,
                timeout=5
            )
            short_path = result.stdout.strip().strip('"')
            if short_path and os.path.exists(short_path):
                return short_path
        except:
            pass
    return long_path

# Detect if running in Docker
IN_DOCKER = os.path.exists('/.dockerenv') or os.getenv('IN_DOCKER', 'false').lower() == 'true'

if not IN_DOCKER:
    java_home_long = r'D:\jdk17'
    java_home_short = get_short_path(java_home_long)
    os.environ['JAVA_HOME'] = java_home_short if java_home_short != java_home_long else java_home_long
    os.environ['HADOOP_HOME'] = r'D:\hadoopbin\winutils\hadoop-3.0.1'
    os.environ['SPARK_HOME'] = r'D:\DOWNLOAD\spark-3.5.7-bin-hadoop3\spark-3.5.7-bin-hadoop3'
    java_exe = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java.exe')
    if not os.path.exists(java_exe):
        raise FileNotFoundError(f"Java not found at {java_exe}.")
else:
    print("[INFO] Running in Docker container - using container's Java/Spark configuration")

# Java security handling (same as other scripts)
if not IN_DOCKER:
    java_security_dir = os.path.join(os.environ['JAVA_HOME'], 'lib', 'security')
    java_security_file = os.path.join(java_security_dir, 'java.security')
    temp_security_file = None
    security_file_found = os.path.exists(java_security_file)
    
    if security_file_found:
        try:
            file_size = os.path.getsize(java_security_file)
            if file_size < 100:
                security_file_found = False
        except:
            security_file_found = False
else:
    security_file_found = True
    temp_security_file = None

if not IN_DOCKER:
    print("[INFO] Creating temporary security file as fallback...")
    try:
        temp_dir = tempfile.gettempdir()
        temp_security_dir = os.path.join(temp_dir, "java_security")
        os.makedirs(temp_security_dir, exist_ok=True)
        temp_security_file = os.path.join(temp_security_dir, 'java.security')
        
        minimal_security_content = """# Minimal Java Security Configuration
security.provider.1=SUN
security.provider.2=SunRsaSign
security.provider.3=SunEC
security.provider.4=SunJSSE
security.provider.5=SunJCE
security.provider.6=SunJGSS
security.provider.7=SunSASL
security.provider.8=XMLDSig
security.provider.9=SunPCSC
security.provider.10=JdkLDAP
security.provider.11=JdkSASL
security.provider.12=SunPKCS11
policy.url.1=file:${java.home}/lib/security/java.policy
policy.url.2=file:${user.home}/.java.policy
keystore.type=jks
"""
        with open(temp_security_file, 'w') as f:
            f.write(minimal_security_content)
        print(f"[OK] Created temporary security file")
    except Exception as e:
        print(f"[WARN] Could not create temporary security file: {e}")
        temp_security_file = None

if not IN_DOCKER:
    java_bin = os.path.join(os.environ['JAVA_HOME'], 'bin')
    hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
    current_path = os.environ.get('PATH', '')
    os.environ['PATH'] = f"{java_bin};{hadoop_bin};{current_path}"
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

temp_dir = tempfile.gettempdir()
spark_warehouse = os.path.join(temp_dir, "spark_warehouse")
spark_local = os.path.join(temp_dir, "spark_local")

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDFModel
from pyspark.sql.functions import col, udf, input_file_name, count, max as spark_max, avg, desc, current_timestamp
from pyspark.sql.types import ArrayType, StringType
import re

from config.hdfs_config import (
    HDFS_STREAM_INPUT, HDFS_REFERENCE_VECTORS, HDFS_IDF_MODEL,
    HDFS_STREAMING_VECTORS, HDFS_PLAGIARISM_RESULTS, HDFS_REPORTS,
    HDFS_BASE_URL, get_spark_hdfs_config
)
from scripts.similarity import find_plagiarism_candidates

# Import Elasticsearch for indexing
try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
    ES_AVAILABLE = True
except ImportError:
    print("[WARN] Elasticsearch client not available. Indexing will be skipped.")
    ES_AVAILABLE = False

from config.elasticsearch_config import ES_URL, ES_INDEX_PLAGIARISM, ES_INDEX_ANALYSIS

# Configuration
PLAGIARISM_THRESHOLD = 0.7
TOP_K_RESULTS = 10
BATCH_SIZE = 10  # Files per trigger
TRIGGER_INTERVAL = '5 seconds'

print("\n" + "="*70)
print("  NETPLAG - FULL STREAM PROCESS")
print("="*70)
print("Complete pipeline: Streaming → Analysis → Elasticsearch Indexing")
print(f"[INFO] Environment: {'Docker' if IN_DOCKER else 'Windows Host'}")
print(f"[INFO] Plagiarism threshold: {PLAGIARISM_THRESHOLD}")
print(f"[INFO] Top-K results: {TOP_K_RESULTS}")
print(f"[INFO] Batch size: {BATCH_SIZE} files per trigger")
print(f"[INFO] Trigger interval: {TRIGGER_INTERVAL}")
print("="*70 + "\n")

# Initialize Spark
if not IN_DOCKER:
    java_home_path = os.environ.get('JAVA_HOME', '').replace('\\', '/')
    java_security_options_parts = [
        f"-Djava.home={java_home_path}",
        "-Dfile.encoding=UTF-8",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+UseG1GC",
        "-Djava.awt.headless=true"
    ]
    if not security_file_found:
        java_security_options_parts.extend([
            "-Djava.security.manager=",
            "-Djava.security.policy=",
            "-XX:+DisableAttachMechanism"
        ])
    if temp_security_file:
        temp_security_path = temp_security_file.replace('\\', '/')
        if temp_security_path[1] == ':':
            temp_security_path = '/' + temp_security_path.replace(':', '')
        java_security_options_parts.append(f"-Djava.security.properties=file://{temp_security_path}")
    
    java_security_options = " ".join(java_security_options_parts)
    
    spark_config = {
        "spark.app.name": "NetPlag_Full_Stream_Process",
        "spark.master": "local[4]",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        "spark.sql.shuffle.partitions": "50",
        "spark.driver.extraJavaOptions": java_security_options,
        "spark.executor.extraJavaOptions": java_security_options,
        "spark.sql.warehouse.dir": spark_warehouse,
        "spark.local.dir": spark_local,
        "spark.executorEnv.JAVA_HOME": os.environ['JAVA_HOME']
    }
    if 'SPARK_HOME' in os.environ:
        del os.environ['SPARK_HOME']
else:
    spark_config = {
        "spark.app.name": "NetPlag_Full_Stream_Process",
        "spark.master": "local[4]",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        "spark.sql.shuffle.partitions": "50"
    }

spark_config.update(get_spark_hdfs_config())

# Disable native IO for Windows to fix checkpoint access issue
if not IN_DOCKER:
    spark_config["spark.hadoop.io.native.lib.available"] = "false"
    spark_config["spark.hadoop.hadoop.tmp.dir"] = os.path.join(tempfile.gettempdir(), "hadoop_tmp")

print("[STEP 1/7] Initialisation de Spark...")
try:
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
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Verify HDFS connection
print("\n[STEP 2/7] Vérification de la connexion HDFS...")
try:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    if not IN_DOCKER:
        hadoop_conf.set("dfs.client.use.datanode.hostname", "true")
        hadoop_conf.set("dfs.datanode.use.datanode.hostname", "true")
        # Force DataNode to advertise hostname that resolves to localhost
        hadoop_conf.set("dfs.datanode.hostname", "datanode")
        hadoop_conf.set("dfs.client.socket-timeout", "60000")
        hadoop_conf.set("dfs.datanode.socket.write.timeout", "60000")
        # Additional settings to help with Windows networking
        hadoop_conf.set("dfs.client.read.shortcircuit", "false")
        hadoop_conf.set("dfs.domain.socket.path", "")
        # Disable native IO to fix Windows checkpoint access issue
        hadoop_conf.set("io.native.lib.available", "false")
        # Set Hadoop temp directory
        hadoop_conf.set("hadoop.tmp.dir", os.path.join(tempfile.gettempdir(), "hadoop_tmp"))
    
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI(HDFS_BASE_URL),
        hadoop_conf
    )
    root_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path("/")
    fs.exists(root_path)
    print(f"[OK] Connexion HDFS réussie: {HDFS_BASE_URL}")
except Exception as e:
    print(f"[ERROR] Impossible de se connecter à HDFS: {e}")
    print(f"[ERROR] Vérifiez que HDFS est démarré: docker-compose up -d")
    spark.stop()
    sys.exit(1)

# Initialize Elasticsearch
es = None
if ES_AVAILABLE:
    print("\n[STEP 3/7] Connexion à Elasticsearch...")
    try:
        es = Elasticsearch([ES_URL], request_timeout=30)
        if es.ping():
            print(f"[OK] Connecté à Elasticsearch: {ES_URL}")
        else:
            print("[WARN] Elasticsearch ping failed, indexing will be skipped")
            es = None
    except Exception as e:
        print(f"[WARN] Impossible de se connecter à Elasticsearch: {e}")
        print("[WARN] Indexing will be skipped, but processing will continue")
        es = None

# Load reference data
print("\n[STEP 4/7] Chargement des données de référence...")
try:
    df_reference = spark.read.parquet(HDFS_REFERENCE_VECTORS)
    num_refs = df_reference.count()
    if num_refs == 0:
        print("[ERROR] No reference vectors found!")
        spark.stop()
        sys.exit(1)
    print(f"[OK] {num_refs} documents de référence chargés")
    
    try:
        idf_model = IDFModel.load(HDFS_IDF_MODEL)
        print(f"[OK] Modèle IDF chargé")
    except Exception as e:
        print(f"[WARN] Impossible de charger le modèle IDF: {e}")
        idf_model = None
    
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=5000)
except Exception as e:
    print(f"[ERROR] Erreur lors du chargement: {e}")
    import traceback
    traceback.print_exc()
    spark.stop()
    sys.exit(1)

# Text cleaning function
def clean_text(text):
    if not text: 
        return []
    text = text.lower()[:50000]
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    words = text.split()
    words = [w for w in words if len(w) > 2]
    return words

clean_udf = udf(clean_text, ArrayType(StringType()))

# Global variable for streaming query
streaming_query = None

# Function to run batch analysis (Step 3)
def run_batch_analysis():
    """Run batch analysis on all streaming vectors"""
    try:
        print("\n[ANALYSIS] Démarrage de l'analyse batch...")
        
        # Load streaming vectors
        try:
            df_streaming = spark.read.parquet(HDFS_STREAMING_VECTORS)
            num_streaming = df_streaming.count()
            if num_streaming == 0:
                print("[ANALYSIS] Aucun vecteur streaming à analyser")
                return
            print(f"[ANALYSIS] {num_streaming} vecteurs streaming chargés")
        except Exception as e:
            print(f"[ANALYSIS] Aucun vecteur streaming trouvé: {e}")
            return
        
        # Calculate similarity
        print("[ANALYSIS] Calcul de similarité...")
        plagiarism_results = find_plagiarism_candidates(
            df_streaming, df_reference,
            threshold=PLAGIARISM_THRESHOLD,
            top_k=TOP_K_RESULTS,
            vec_col="features",
            id_col="filename"
        )
        
        plagiarism_results.cache()
        total_comparisons = plagiarism_results.count()
        plagiarism_cases = plagiarism_results.filter(col("is_plagiarism") == True).count()
        
        print(f"[ANALYSIS] ✓ Analyse terminée: {total_comparisons} comparaisons, {plagiarism_cases} cas de plagiat")
        
        # Generate summary
        summary_df = plagiarism_results.filter(col("is_plagiarism") == True) \
            .groupBy("document_filename") \
            .agg(
                count("*").alias("num_matches"),
                spark_max("similarity_score").alias("max_score"),
                avg("similarity_score").alias("avg_score")
            ) \
            .orderBy(desc("max_score"))
        
        # Save reports
        print("[ANALYSIS] Sauvegarde des rapports...")
        plagiarism_results.write \
            .mode("overwrite") \
            .parquet(f"{HDFS_REPORTS}/detailed_results")
        
        summary_df.write \
            .mode("overwrite") \
            .parquet(f"{HDFS_REPORTS}/summary")
        
        plagiarism_results.filter(col("is_plagiarism") == True) \
            .orderBy(col("similarity_score").desc()) \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .json(f"{HDFS_REPORTS}/plagiarism_cases.json")
        
        print("[ANALYSIS] ✓ Rapports sauvegardés")
        
        return plagiarism_results, summary_df
        
    except Exception as e:
        print(f"[ANALYSIS] Erreur lors de l'analyse: {e}")
        import traceback
        traceback.print_exc()
        return None, None

# Function to index to Elasticsearch (Step 5)
def index_to_elasticsearch(plagiarism_results, summary_df):
    """Index results to Elasticsearch"""
    if not es or not ES_AVAILABLE:
        print("[INDEXING] Elasticsearch non disponible, indexation ignorée")
        return
    
    try:
        print("\n[INDEXING] Démarrage de l'indexation Elasticsearch...")
        
        # Index plagiarism results
        if plagiarism_results:
            print("[INDEXING] Indexation des résultats de plagiat...")
            rows = plagiarism_results.collect()
            
            actions = []
            for row in rows:
                doc = {
                    "_index": ES_INDEX_PLAGIARISM,
                    "_source": {
                        "document_filename": row["document_filename"],
                        "reference_filename": row["reference_filename"],
                        "similarity_score": float(row["similarity_score"]),
                        "is_plagiarism": bool(row["is_plagiarism"]),
                        "timestamp": datetime.now().isoformat()
                    }
                }
                actions.append(doc)
            
            if actions:
                success, failed = bulk(es, actions, chunk_size=1000)
                print(f"[INDEXING] ✓ {success} documents indexés")
                es.indices.refresh(index=ES_INDEX_PLAGIARISM)
        
        # Index summary
        if summary_df:
            print("[INDEXING] Indexation du résumé...")
            rows = summary_df.collect()
            
            actions = []
            for row in rows:
                doc = {
                    "_index": ES_INDEX_ANALYSIS,
                    "_source": {
                        "document_filename": row["document_filename"],
                        "num_matches": int(row["num_matches"]),
                        "max_score": float(row["max_score"]),
                        "avg_score": float(row["avg_score"]),
                        "analysis_timestamp": datetime.now().isoformat()
                    }
                }
                actions.append(doc)
            
            if actions:
                success, failed = bulk(es, actions, chunk_size=1000)
                print(f"[INDEXING] ✓ {success} résumés indexés")
                es.indices.refresh(index=ES_INDEX_ANALYSIS)
        
        print("[INDEXING] ✓ Indexation terminée")
        
    except Exception as e:
        print(f"[INDEXING] Erreur lors de l'indexation: {e}")
        import traceback
        traceback.print_exc()

# Process each batch
def process_batch(df_batch, batch_id):
    """Process a batch of files - Step 2 functionality"""
    file_count = df_batch.count()
    print(f"\n{'='*70}")
    print(f"--- Traitement du Batch ID: {batch_id} ---")
    print(f"[DEBUG] Fichiers détectés dans ce batch: {file_count}")
    
    if file_count == 0:
        print("[DEBUG] Aucun fichier à traiter dans ce batch")
        return
    
    print(f"\n{'='*70}")
    print(f"  TRAITEMENT BATCH #{batch_id}")
    print(f"{'='*70}")
    
    try:
        # Apply TF hashing
        tf_batch = hashingTF.transform(df_batch)
        
        # Apply IDF transform
        if idf_model is not None:
            df_vectors = idf_model.transform(tf_batch).select("filename", col("features").alias("features"))
        else:
            df_vectors = tf_batch.select("filename", col("rawFeatures").alias("features"))
        
        count = df_vectors.count()
        print(f"[BATCH #{batch_id}] ✓ {count} document(s) vectorisé(s)")
        
        # Calculate similarity
        print(f"[BATCH #{batch_id}] Calcul de similarité...")
        plagiarism_results = find_plagiarism_candidates(
            df_vectors, df_reference,
            threshold=PLAGIARISM_THRESHOLD,
            top_k=TOP_K_RESULTS,
            vec_col="features",
            id_col="filename"
        )
        
        plagiarism_count = plagiarism_results.filter(col("is_plagiarism") == True).count()
        print(f"[BATCH #{batch_id}] ✓ {plagiarism_count} cas de plagiat détecté(s)")
        
        # Save to HDFS
        plagiarism_results.write \
            .mode("append") \
            .parquet(HDFS_PLAGIARISM_RESULTS)
        
        df_vectors.write \
            .mode("append") \
            .parquet(HDFS_STREAMING_VECTORS)
        
        print(f"[BATCH #{batch_id}] ✓ Données sauvegardées sur HDFS")
        
        # Run batch analysis (Step 3)
        print(f"\n[BATCH #{batch_id}] Exécution de l'analyse batch...")
        analysis_results, summary_df = run_batch_analysis()
        
        # Index to Elasticsearch (Step 5)
        if analysis_results is not None:
            print(f"\n[BATCH #{batch_id}] Indexation vers Elasticsearch...")
            index_to_elasticsearch(analysis_results, summary_df)
        
        print(f"\n[BATCH #{batch_id}] ✓ Traitement complet terminé!")
        print(f"{'='*70}\n")
        
    except Exception as e:
        print(f"[BATCH #{batch_id}] ⚠ Erreur: {e}")
        import traceback
        traceback.print_exc()

# Setup streaming source
print("\n[STEP 5/7] Configuration de la source streaming...")
try:
    stream_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(HDFS_STREAM_INPUT.replace(HDFS_BASE_URL, ""))
    if not fs.exists(stream_path):
        print(f"[INFO] Création du répertoire: {HDFS_STREAM_INPUT}")
        fs.mkdirs(stream_path)
except Exception as e:
    print(f"[WARN] Could not verify stream input directory: {e}")

df_stream = spark.readStream \
    .format("text") \
    .option("wholetext", "true") \
    .option("maxFilesPerTrigger", BATCH_SIZE) \
    .load(f"{HDFS_STREAM_INPUT}/*.txt") \
    .withColumn("filename", input_file_name())

df_clean = df_stream.withColumn("words", clean_udf(col("value")))

# Use HDFS for checkpoint to avoid Windows native IO issues
checkpoint_uri = f"{HDFS_BASE_URL}/netplag/storage/streaming_checkpoint_full"

# Create checkpoint directory on HDFS if it doesn't exist
try:
    checkpoint_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path("/netplag/storage/streaming_checkpoint_full")
    if not fs.exists(checkpoint_path):
        fs.mkdirs(checkpoint_path)
        print(f"[INFO] Created HDFS checkpoint directory: {checkpoint_uri}")
    else:
        print(f"[INFO] Using existing HDFS checkpoint directory: {checkpoint_uri}")
except Exception as e:
    print(f"[WARN] Could not verify/create HDFS checkpoint directory: {e}")

# Cleanup function
def cleanup():
    global streaming_query
    if streaming_query is not None:
        try:
            print("\n[INFO] Arrêt du streaming...")
            streaming_query.stop()
        except:
            pass
    try:
        spark.stop()
    except:
        pass

atexit.register(cleanup)

def signal_handler(sig, frame):
    print("\n\n[INFO] Interruption reçue, arrêt en cours...")
    cleanup()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
if hasattr(signal, 'SIGTERM'):
    signal.signal(signal.SIGTERM, signal_handler)

# Start streaming
print("\n[STEP 6/7] Démarrage du streaming...")
print(f"[INFO] Surveillance: {HDFS_STREAM_INPUT}")
print(f"[INFO] Checkpoint: {checkpoint_uri}")
print("\n[STEP 7/7] 🔴 STREAMING ACTIF - Pipeline complet en cours d'exécution")
print("="*70)
print("Pipeline: Détection → Traitement → Analyse → Indexation")
print("="*70)
print("Appuyez sur Ctrl+C pour arrêter\n")

try:
    streaming_query = df_clean.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .option("checkpointLocation", checkpoint_uri) \
        .start()
    
    streaming_query.awaitTermination()
except KeyboardInterrupt:
    print("\n[INFO] Interruption clavier reçue")
    cleanup()
except Exception as e:
    print(f"\n[ERROR] Erreur streaming: {e}")
    import traceback
    traceback.print_exc()
    cleanup()
    sys.exit(1)

