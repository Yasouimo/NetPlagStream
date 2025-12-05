"""
Script d'analyse batch de plagiat
Charge les vecteurs streaming et référence depuis HDFS,
calcule la similarité pour tous les documents,
et génère un rapport avec scores et documents suspects
"""

import os
import sys
import tempfile
import subprocess

# Try to get short path (8.3 format) to avoid space issues
def get_short_path(long_path):
    """Get Windows 8.3 short path format to avoid space issues"""
    try:
        import win32api
        return win32api.GetShortPathName(long_path)
    except ImportError:
        # If pywin32 not available, try using subprocess
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
    # Only set these when running on Windows host
    java_home_long = r'D:\jdk17'
    java_home_short = get_short_path(java_home_long)
    os.environ['JAVA_HOME'] = java_home_short if java_home_short != java_home_long else java_home_long
    
    os.environ['HADOOP_HOME'] = r'D:\hadoopbin\winutils\hadoop-3.0.1'
    os.environ['SPARK_HOME'] = r'D:\DOWNLOAD\spark-3.5.7-bin-hadoop3\spark-3.5.7-bin-hadoop3'
    
    # Verify Java installation
    java_exe = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java.exe')
    if not os.path.exists(java_exe):
        raise FileNotFoundError(f"Java not found at {java_exe}. Please verify JAVA_HOME is correct.")
else:
    # In Docker, Java and Spark are already configured
    print("[INFO] Running in Docker container - using container's Java/Spark configuration")

# Test Java execution to catch security issues early (only on Windows host)
if not IN_DOCKER:
    print("[INFO] Testing Java installation...")
    try:
        result = subprocess.run(
            [java_exe, '-version'],
            capture_output=True,
            text=True,
            timeout=10,
            env=os.environ.copy()
        )
        if result.returncode == 0:
            print(f"[OK] Java is working: {result.stderr.split(chr(10))[0] if result.stderr else 'Version check passed'}")
        else:
            print(f"[WARN] Java version check returned non-zero: {result.stderr}")
    except subprocess.TimeoutExpired:
        print("[WARN] Java version check timed out")
    except Exception as e:
        print(f"[WARN] Java version check failed: {e}")
        print("[WARN] This may indicate Java installation issues, but continuing...")

# Verify Java security file exists and create minimal one if missing (only on Windows host)
if not IN_DOCKER:
    java_security_dir = os.path.join(os.environ['JAVA_HOME'], 'lib', 'security')
    java_security_file = os.path.join(java_security_dir, 'java.security')
    temp_security_file = None
    security_file_found = os.path.exists(java_security_file)
    
    # Check if file exists but is empty or too small (likely corrupted)
    if security_file_found:
        try:
            file_size = os.path.getsize(java_security_file)
            if file_size < 100:  # Security file should be at least 100 bytes
                print(f"[WARN] Java security file exists but is too small ({file_size} bytes), treating as missing")
                security_file_found = False
            else:
                # Try to read first few lines to verify it's not corrupted
                with open(java_security_file, 'r', encoding='utf-8', errors='ignore') as f:
                    first_line = f.readline()
                    if not first_line or first_line.strip() == '':
                        print(f"[WARN] Java security file appears empty or corrupted, treating as missing")
                        security_file_found = False
        except Exception as e:
            print(f"[WARN] Could not verify Java security file: {e}, treating as missing")
            security_file_found = False
else:
    # In Docker, assume Java is properly configured
    security_file_found = True
    temp_security_file = None

if not IN_DOCKER:
    # Always create a temp security file as fallback, even if original exists
    # This helps when the original file is corrupted or Java can't load it
    print("[INFO] Creating temporary security file as fallback...")
    try:
        temp_dir = tempfile.gettempdir()
        temp_security_dir = os.path.join(temp_dir, "java_security")
        os.makedirs(temp_security_dir, exist_ok=True)
        temp_security_file = os.path.join(temp_security_dir, 'java.security')
        
        # Create comprehensive java.security file
        minimal_security_content = """# Minimal Java Security Configuration
# This file was auto-generated to fix Java 17 security initialization issues
#
# Security providers
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

# Policy files
policy.url.1=file:${java.home}/lib/security/java.policy
policy.url.2=file:${user.home}/.java.policy

# Keystore
keystore.type=jks
"""
        with open(temp_security_file, 'w') as f:
            f.write(minimal_security_content)
        print(f"[OK] Created temporary security file at {temp_security_file}")
        
        if not security_file_found:
            print(f"[WARN] Original Java security file not found or corrupted at {java_security_file}")
            print("[WARN] Using temporary security file as fallback")
        else:
            print(f"[OK] Original Java security file found at {java_security_file}")
            print("[INFO] Temporary security file created as additional fallback")
    except Exception as e:
        print(f"[WARN] Could not create temporary security file: {e}")
        temp_security_file = None
        if not security_file_found:
            print(f"[ERROR] Java security file not found and could not create fallback")
            print("[ERROR] This may cause Spark initialization to fail")

# Ajouter Java et Hadoop au PATH (only on Windows host)
if not IN_DOCKER:
    java_bin = os.path.join(os.environ['JAVA_HOME'], 'bin')
    hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
    current_path = os.environ.get('PATH', '')
    os.environ['PATH'] = f"{java_bin};{hadoop_bin};{current_path}"
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
else:
    # In Docker, Python paths are already set
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configurer des chemins temporaires sans espaces pour éviter les problèmes
temp_dir = tempfile.gettempdir()
# Use Windows paths directly (Spark handles Windows paths correctly)
spark_warehouse = os.path.join(temp_dir, "spark_warehouse")
spark_local = os.path.join(temp_dir, "spark_local")

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import Spark AFTER all environment variables are set
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, avg, desc
from config.hdfs_config import (
    HDFS_STREAMING_VECTORS, HDFS_REFERENCE_VECTORS, HDFS_REPORTS,
    HDFS_BASE_URL, get_spark_hdfs_config
)
from scripts.similarity import find_plagiarism_candidates

# Configuration
PLAGIARISM_THRESHOLD = 0.7
TOP_K_RESULTS = 10

print("\n" + "="*60)
print("  NETPLAG - ANALYSE BATCH DE PLAGIAT")
print("="*60)
print(f"[INFO] Seuil de plagiat: {PLAGIARISM_THRESHOLD}")
print(f"[INFO] Top-K résultats: {TOP_K_RESULTS}")
print(f"[INFO] Environnement: {'Docker' if IN_DOCKER else 'Windows Host'}")
print("="*60 + "\n")

# Initialisation Spark avec configuration HDFS
# Java security options (only needed on Windows host)
if not IN_DOCKER:
    # Java 17 security options to fix "Error loading java.security file"
    # Try multiple approaches to work around missing/corrupted security files
    java_home_path = os.environ.get('JAVA_HOME', '').replace('\\', '/')
    
    # Build Java security options
    # For Java 17 with missing security file, try multiple workarounds
    java_security_options_parts = [
        f"-Djava.home={java_home_path}",
        "-Dfile.encoding=UTF-8",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+UseG1GC",
        "-Djava.awt.headless=true"
    ]
    
    # If security file is missing or corrupted, try aggressive workarounds
    if not security_file_found:
        # Try to prevent security manager initialization
        # Note: These may not work as Security.initialize() is called very early
        java_security_options_parts.extend([
            "-Djava.security.manager=",
            "-Djava.security.policy=",
            "-XX:+DisableAttachMechanism"
        ])
        print("[WARN] Using workaround JVM options (may not fully resolve the issue)")
    
    # Always try to use temp security file if available (as fallback or supplement)
    # Note: -Djava.security.properties adds to existing properties, doesn't replace base file
    # But it's worth trying as a last resort
    if temp_security_file and os.path.exists(temp_security_file):
        temp_security_path = temp_security_file.replace('\\', '/')
        # Convert Windows path to file:// URL format
        if temp_security_path[1] == ':':  # Windows absolute path like C:/...
            temp_security_path = '/' + temp_security_path.replace(':', '')
        java_security_options_parts.append(f"-Djava.security.properties=file://{temp_security_path}")
        print(f"[INFO] Using custom security properties from: {temp_security_file}")
    
    java_security_options = " ".join(java_security_options_parts)
    
    spark_config = {
        "spark.app.name": "NetPlag_Plagiarism_Analysis",
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
    # Remove SPARK_HOME from environment when in Docker (it's set by the container)
    if 'SPARK_HOME' in os.environ:
        del os.environ['SPARK_HOME']
else:
    # In Docker, minimal configuration
    spark_config = {
        "spark.app.name": "NetPlag_Plagiarism_Analysis",
        "spark.master": "local[4]",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        "spark.sql.shuffle.partitions": "50"
    }

# Add HDFS configuration
spark_config.update(get_spark_hdfs_config())

print("[STEP 1/6] Initialisation de Spark...")
try:
    # Stop any existing Spark session
    try:
        spark_old = SparkSession.getActiveSession()
        if spark_old:
            print("[INFO] Arrêt de la session Spark existante...")
            spark_old.stop()
    except:
        pass
    
    # Build SparkSession
    spark_builder = SparkSession.builder
    for key, value in spark_config.items():
        spark_builder = spark_builder.config(key, value)
    spark = spark_builder.getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f"[OK] Spark initialisé avec succès")
    print(f"     - Master: {spark.sparkContext.master}")
    print(f"     - App Name: {spark.conf.get('spark.app.name')}")
except Exception as e:
    print(f"[ERROR] Échec de l'initialisation Spark: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Verify HDFS connection
print("\n[STEP 2/6] Vérification de la connexion HDFS...")
try:
    from pyspark.sql import Row
    # Try to list root directory to verify HDFS connection
    test_df = spark.read.text(f"{HDFS_BASE_URL}/")
    test_df.count()
    print(f"[OK] Connexion HDFS réussie: {HDFS_BASE_URL}")
except Exception as e:
    print(f"[ERROR] Impossible de se connecter à HDFS: {e}")
    print(f"[ERROR] Vérifiez que HDFS est démarré: docker-compose up -d")
    print(f"[ERROR] URL HDFS: {HDFS_BASE_URL}")
    spark.stop()
    exit(1)

# 1. Load vectors from HDFS
print("\n[STEP 3/6] Chargement des données depuis HDFS...")
print(f"     - Vecteurs streaming: {HDFS_STREAMING_VECTORS}")
print(f"     - Vecteurs de référence: {HDFS_REFERENCE_VECTORS}")
try:
    df_streaming = spark.read.parquet(HDFS_STREAMING_VECTORS)
    num_streaming = df_streaming.count()
    print(f"[OK] Vecteurs streaming chargés: {num_streaming} documents")
    if num_streaming > 0:
        print(f"     - Colonnes: {', '.join(df_streaming.columns)}")
        print(f"     - Exemple de document: {df_streaming.select('filename').first()[0] if 'filename' in df_streaming.columns else 'N/A'}")
    
    df_reference = spark.read.parquet(HDFS_REFERENCE_VECTORS)
    num_reference = df_reference.count()
    print(f"[OK] Vecteurs de référence chargés: {num_reference} documents")
    if num_reference > 0:
        print(f"     - Colonnes: {', '.join(df_reference.columns)}")
        print(f"     - Exemple de référence: {df_reference.select('filename').first()[0] if 'filename' in df_reference.columns else 'N/A'}")
    
except Exception as e:
    print(f"[ERROR] Erreur lors du chargement: {e}")
    import traceback
    traceback.print_exc()
    spark.stop()
    exit(1)

if num_streaming == 0:
    print("[WARN] Aucun document streaming à analyser")
    print("[INFO] Exécutez d'abord le script de streaming (2_streaming_app.py) pour générer des vecteurs streaming")
    spark.stop()
    exit(0)

if num_reference == 0:
    print("[ERROR] Aucun vecteur de référence trouvé")
    print("[ERROR] Exécutez d'abord le script d'initialisation batch (1_batch_init.py)")
    spark.stop()
    exit(1)

# 2. Calculate similarity
print(f"\n[STEP 4/6] Calcul de similarité...")
print(f"     - Seuil: {PLAGIARISM_THRESHOLD}")
print(f"     - Top-K: {TOP_K_RESULTS}")
print(f"     - Comparaisons: {num_streaming} documents × {num_reference} références = {num_streaming * num_reference:,} comparaisons possibles")
print("     [INFO] Cela peut prendre du temps...")

try:
    plagiarism_results = find_plagiarism_candidates(
        df_streaming, df_reference,
        threshold=PLAGIARISM_THRESHOLD,
        top_k=TOP_K_RESULTS,
        vec_col="features",
        id_col="filename"
    )
    
    # Cache results for multiple operations
    plagiarism_results.cache()
    
    total_comparisons = plagiarism_results.count()
    plagiarism_cases = plagiarism_results.filter(col("is_plagiarism") == True).count()
    
    print(f"[OK] Analyse terminée avec succès!")
    print(f"     - Comparaisons effectuées: {total_comparisons:,}")
    print(f"     - Cas de plagiat détectés: {plagiarism_cases:,}")
    if total_comparisons > 0:
        plagiarism_rate = (plagiarism_cases / total_comparisons) * 100
        print(f"     - Taux de plagiat: {plagiarism_rate:.2f}%")
    
except Exception as e:
    print(f"[ERROR] Erreur lors du calcul de similarité: {e}")
    import traceback
    traceback.print_exc()
    spark.stop()
    exit(1)

# 3. Generate statistics
print("\n[STEP 5/6] Génération des statistiques...")

try:
    stats = plagiarism_results.filter(col("is_plagiarism") == True).agg(
        count("*").alias("total_plagiarism_cases"),
        spark_max("similarity_score").alias("max_similarity"),
        avg("similarity_score").alias("avg_similarity")
    ).collect()[0]
    
    print(f"[OK] Statistiques générées:")
    print(f"\n{'='*60}")
    print(f"  STATISTIQUES DE PLAGIAT")
    print(f"{'='*60}")
    print(f"  Total cas de plagiat: {stats['total_plagiarism_cases']:,}")
    if stats['max_similarity'] is not None:
        print(f"  Similarité maximale: {stats['max_similarity']:.4f}")
        print(f"  Similarité moyenne: {stats['avg_similarity']:.4f}")
    print(f"{'='*60}\n")
    
except Exception as e:
    print(f"[WARN] Erreur lors de la génération des statistiques: {e}")
    stats = None

# 4. Show top plagiarism cases
print("[INFO] Affichage des top cas de plagiat...")
try:
    top_cases = plagiarism_results.filter(col("is_plagiarism") == True) \
        .orderBy(col("similarity_score").desc()) \
        .limit(20)
    
    top_count = top_cases.count()
    if top_count > 0:
        print(f"\n[OK] Top {min(20, top_count)} cas de plagiat détectés:")
        top_cases.show(20, truncate=False)
    else:
        print("[INFO] Aucun cas de plagiat détecté au-dessus du seuil")
except Exception as e:
    print(f"[WARN] Erreur lors de l'affichage des cas: {e}")

# 5. Generate detailed report
print("\n[STEP 6/6] Génération du rapport détaillé...")

try:
    # Create summary per document
    # Note: plagiarism_results has columns: document_filename, reference_filename, similarity_score, is_plagiarism
    summary_df = plagiarism_results.filter(col("is_plagiarism") == True) \
        .groupBy("document_filename") \
        .agg(
            count("*").alias("num_matches"),
            spark_max("similarity_score").alias("max_score"),
            avg("similarity_score").alias("avg_score")
        ) \
        .orderBy(desc("max_score"))
    
    summary_count = summary_df.count()
    print(f"[OK] Résumé généré: {summary_count} documents avec plagiat détecté")
    
    if summary_count > 0:
        print(f"\n[INFO] Top documents suspects (par score maximum):")
        summary_df.show(50, truncate=False)
    
except Exception as e:
    print(f"[ERROR] Erreur lors de la génération du résumé: {e}")
    import traceback
    traceback.print_exc()
    summary_df = None

# 6. Save report to HDFS
print(f"\n[INFO] Sauvegarde du rapport vers HDFS...")
print(f"     - Répertoire: {HDFS_REPORTS}")

try:
    # Ensure HDFS directory exists
    from pyspark.sql import Row
    # Try to create directory by writing a dummy file (Spark will create parent dirs)
    dummy_df = spark.createDataFrame([Row(dummy="check")])
    dummy_df.write.mode("overwrite").parquet(f"{HDFS_REPORTS}/.dummy_check")
    spark.read.parquet(f"{HDFS_REPORTS}/.dummy_check").count()
    # Remove dummy file
    # (We can't easily delete via Spark, but it's small and harmless)
    
    # Save detailed results
    print(f"     - Sauvegarde des résultats détaillés...")
    plagiarism_results.write \
        .mode("overwrite") \
        .parquet(f"{HDFS_REPORTS}/detailed_results")
    print(f"[OK] Résultats détaillés sauvegardés: {HDFS_REPORTS}/detailed_results")
    
    # Save summary
    if summary_df is not None:
        print(f"     - Sauvegarde du résumé...")
        summary_df.write \
            .mode("overwrite") \
            .parquet(f"{HDFS_REPORTS}/summary")
        print(f"[OK] Résumé sauvegardé: {HDFS_REPORTS}/summary")
    
    # Also save as JSON for easier reading
    print(f"     - Sauvegarde au format JSON...")
    plagiarism_results.filter(col("is_plagiarism") == True) \
        .orderBy(col("similarity_score").desc()) \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .json(f"{HDFS_REPORTS}/plagiarism_cases.json")
    print(f"[OK] Rapport JSON sauvegardé: {HDFS_REPORTS}/plagiarism_cases.json")
    
    print(f"\n[OK] Tous les rapports ont été sauvegardés avec succès!")
    
except Exception as e:
    print(f"[ERROR] Erreur lors de la sauvegarde: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("  ANALYSE TERMINÉE AVEC SUCCÈS!")
print("="*60)
print(f"✓ Rapports disponibles dans: {HDFS_REPORTS}")
print(f"  - Résultats détaillés: {HDFS_REPORTS}/detailed_results")
if summary_df is not None:
    print(f"  - Résumé: {HDFS_REPORTS}/summary")
print(f"  - Rapport JSON: {HDFS_REPORTS}/plagiarism_cases.json")
print("="*60 + "\n")

spark.stop()
print("[OK] Session Spark fermée proprement")
