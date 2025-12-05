"""
Script de création de la structure HDFS
Crée uniquement les répertoires HDFS nécessaires.
La migration des fichiers est gérée par migrate_fast.ps1
La migration des modèles IDF et vecteurs peut être faite avec --migrate-models
"""

import os
import sys
import tempfile

# Detect if running in Docker
IN_DOCKER = os.path.exists('/.dockerenv') or os.getenv('IN_DOCKER', 'false').lower() == 'true'

if not IN_DOCKER:
    # Windows host setup
    os.environ['JAVA_HOME'] = r'D:\jdk17'
    os.environ['HADOOP_HOME'] = r'D:\hadoopbin\winutils\hadoop-3.0.1'
    os.environ['SPARK_HOME'] = r'D:\DOWNLOAD\spark-3.5.7-bin-hadoop3\spark-3.5.7-bin-hadoop3'
    
    # Vérifier que Java existe
    java_exe = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java.exe')
    if not os.path.exists(java_exe):
        raise FileNotFoundError(
            f"Java non trouvé à {java_exe}. "
            "Vérifiez que JAVA_HOME est correct."
        )
    
    # Ajouter Java et Hadoop au PATH (AVANT d'importer Spark)
    # IMPORTANT: Mettre Java en PREMIER dans le PATH pour forcer l'utilisation du bon JDK
    java_bin = os.path.join(os.environ['JAVA_HOME'], 'bin')
    hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
    current_path = os.environ.get('PATH', '')
    # Mettre Java en premier pour qu'il soit trouvé en premier
    os.environ['PATH'] = f"{java_bin};{hadoop_bin};{current_path}"
    
    # Vérifier que Java est accessible
    import subprocess
    try:
        result = subprocess.run([java_exe, '-version'], capture_output=True, text=True, timeout=5)
        print(f"[OK] Java detecte: {os.environ['JAVA_HOME']}")
    except Exception as e:
        print(f"[WARN] Avertissement: Impossible de verifier Java: {e}")
    
    # Verify Java security file exists and create minimal one if missing
    java_security_dir = os.path.join(os.environ['JAVA_HOME'], 'lib', 'security')
    java_security_file = os.path.join(java_security_dir, 'java.security')
    temp_security_file = None
    security_file_found = os.path.exists(java_security_file)
    
    if not security_file_found:
        print(f"[WARN] Java security file not found at {java_security_file}")
        print("[INFO] Attempting workarounds...")
        
        # Check if security directory exists
        if not os.path.exists(java_security_dir):
            print(f"[ERROR] Security directory missing: {java_security_dir}")
            print("[ERROR] Java installation appears corrupted.")
        else:
            print(f"[INFO] Security directory exists but file is missing")
        
        # Try to create security file in temp directory as fallback
        print("[INFO] Creating security file in temp directory...")
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
        except Exception as e:
            print(f"[WARN] Could not create security file: {e}")
    else:
        print(f"[OK] Java security file found at {java_security_file}")
else:
    # In Docker, Java and Spark are already configured
    print("[INFO] Running in Docker container - using container's Java/Spark configuration")
    security_file_found = True
    temp_security_file = None

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configurer des chemins temporaires
import tempfile
temp_dir = tempfile.gettempdir()
# Use paths directly (Spark handles paths correctly)
spark_warehouse = os.path.join(temp_dir, "spark_warehouse")
spark_local = os.path.join(temp_dir, "spark_local")

# Importer Spark APRÈS avoir configuré tous les chemins
print("--- MIGRATION VERS HDFS ---")
print("[INFO] Import de PySpark...")

# Importer la config HDFS AVANT de créer SparkSession
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.hdfs_config import get_spark_hdfs_config

try:
    from pyspark.sql import SparkSession
    from pyspark.conf import SparkConf
    print("[OK] PySpark importe avec succes")
    
    # Get HDFS config
    hdfs_config = get_spark_hdfs_config()
    
    # Créer SparkSession avec configurations
    print("[INFO] Creation de SparkSession...")
    spark_builder = SparkSession.builder \
        .appName("NetPlag_Migration") \
        .master("local[1]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.sql.warehouse.dir", spark_warehouse) \
        .config("spark.local.dir", spark_local)
    
    # Add Windows-specific configs only if not in Docker
    if not IN_DOCKER:
        # Build Java options for security file issues
        java_security_options_parts = [
            "-Djava.security.manager=allow",
            "-Dfile.encoding=UTF-8",
            "-XX:+UnlockExperimentalVMOptions",
            "-XX:+UseG1GC",
            "-Djava.awt.headless=true"
        ]
        
        # Add security file path if we created a temp one
        if temp_security_file:
            java_security_options_parts.append(f"-Djava.security.properties={temp_security_file}")
        
        java_security_options = " ".join(java_security_options_parts)
        
        spark_builder = spark_builder \
            .config("spark.executorEnv.JAVA_HOME", os.environ.get('JAVA_HOME', '')) \
            .config("spark.driver.extraJavaOptions", java_security_options) \
            .config("spark.executor.extraJavaOptions", java_security_options)
    else:
        # In Docker, add minimal Java options
        spark_builder = spark_builder \
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8 -Djava.awt.headless=true") \
            .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Djava.awt.headless=true")
    
    # Add HDFS config
    for key, value in hdfs_config.items():
        spark_builder = spark_builder.config(key, value)
    
    spark = spark_builder.getOrCreate()
    print("[OK] SparkSession creee avec succes!")
    
except Exception as e:
    print(f"[ERROR] Erreur lors de la creation de Spark: {e}")
    import traceback
    traceback.print_exc()
    if not IN_DOCKER:
        print("\n[INFO] Le probleme peut etre cause par:")
        print("  1. Le chemin du repertoire de travail contient des espaces ('work and study')")
        print("  2. Java a des problemes avec les chemins contenant des espaces")
        print("\n[SOLUTION] Utilisez Docker pour la migration:")
        print("  .\\run_spark_docker.ps1 -ScriptPath \"scripts/0_migrate_to_hdfs.py\"")
    sys.exit(1)

# Importer autres modules APRÈS la création de Spark
from config.hdfs_config import *
import glob

spark.sparkContext.setLogLevel("WARN")

# Verify HDFS connection
print("[INFO] Verifying HDFS connection...")
try:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI(HDFS_BASE_URL),
        hadoop_conf
    )
    # Test connection by accessing root
    root_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path("/")
    fs.exists(root_path)
    print(f"[OK] HDFS connection verified: {HDFS_BASE_URL}")
except Exception as e:
    print(f"[ERROR] HDFS connection failed: {e}")
    print(f"\nHDFS URL: {HDFS_BASE_URL}")
    if IN_DOCKER:
        print("\nIn Docker, ensure HDFS containers are running:")
        print("  docker-compose up -d namenode datanode")
    else:
        print("\nOn Windows host, ensure HDFS is accessible:")
        print("  docker ps | findstr hadoop")
    spark.stop()
    sys.exit(1)

def create_hdfs_dirs():
    """Crée la structure de répertoires HDFS"""
    print("\n1. Création de la structure HDFS...")
    
    # Ensure base directory exists first (create via docker exec to avoid permission issues)
    base_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(HDFS_BASE_DIR)
    if not fs.exists(base_path):
        print(f"  [INFO] Création de {HDFS_BASE_DIR} via Docker (pour éviter les problèmes de permissions)...")
        try:
            # Use docker exec to create as root, then set permissions
            import subprocess
            result = subprocess.run(
                ['docker', 'exec', 'hadoop-namenode', 'bash', '-c',
                 f'hdfs dfs -mkdir -p {HDFS_BASE_DIR}/data {HDFS_BASE_DIR}/storage && '
                 f'hdfs dfs -chmod -R 777 {HDFS_BASE_DIR}'],
                capture_output=True,
                timeout=30
            )
            if result.returncode == 0:
                print(f"  ✓ Créé: {HDFS_BASE_DIR} (avec permissions)")
            else:
                print(f"  ⚠ Erreur création via Docker: {result.stderr.decode('utf-8', errors='ignore')[:200]}")
                # Try with Spark API as fallback
                try:
                    fs.mkdirs(base_path)
                    print(f"  ✓ Créé via Spark API: {HDFS_BASE_DIR}")
                except Exception as e2:
                    print(f"  ❌ Échec création: {e2}")
        except Exception as e:
            print(f"  ⚠ Erreur: {e}")
            print(f"  [INFO] Créez manuellement avec:")
            print(f"    docker exec hadoop-namenode hdfs dfs -mkdir -p {HDFS_BASE_DIR}")
            print(f"    docker exec hadoop-namenode hdfs dfs -chmod -R 777 {HDFS_BASE_DIR}")
    else:
        print(f"  - Base directory existe déjà: {HDFS_BASE_DIR}")
    
    dirs = [
        f"{HDFS_BASE_DIR}/data/corpus_initial",
        f"{HDFS_BASE_DIR}/data/stream_input",
        f"{HDFS_BASE_DIR}/data/stream_source",
        f"{HDFS_BASE_DIR}/storage/idf_model",
        f"{HDFS_BASE_DIR}/storage/reference_vectors",
        f"{HDFS_BASE_DIR}/storage/streaming_vectors",
        f"{HDFS_BASE_DIR}/storage/plagiarism_results",
        f"{HDFS_BASE_DIR}/storage/reports"
    ]
    
    for dir_path in dirs:
        hdfs_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(dir_path)
        if not fs.exists(hdfs_path):
            try:
                fs.mkdirs(hdfs_path)
                print(f"  ✓ Créé: {dir_path}")
            except Exception as e:
                print(f"  ❌ Erreur création {dir_path}: {e}")
        else:
            print(f"  - Existe déjà: {dir_path}")

def migrate_corpus():
    """Migre le corpus initial vers HDFS"""
    print("\n2. Migration du corpus initial...")
    
    # In Docker, the local data directory is mounted at /home/jovyan/work/data
    if IN_DOCKER:
        # Check mounted path in Docker
        docker_data_path = "/home/jovyan/work/data/corpus_initial"
        if os.path.exists(docker_data_path):
            corpus_path = docker_data_path
        else:
            # Fallback to config path
            corpus_path = LOCAL_CORPUS_INITIAL
    else:
        corpus_path = LOCAL_CORPUS_INITIAL
    
    if not os.path.exists(corpus_path):
        print(f"  ⚠ Répertoire local non trouvé: {corpus_path}")
        if IN_DOCKER:
            print(f"  [INFO] En Docker, les données doivent être montées dans le conteneur")
            print(f"  [INFO] Vérifiez que le volume est monté dans docker-compose.yml")
        return
    
    txt_files = glob.glob(f"{corpus_path}/*.txt")
    num_files = len(txt_files)
    print(f"  Trouvé {num_files} fichiers à migrer depuis: {corpus_path}")
    
    if num_files == 0:
        print("  ⚠ Aucun fichier à migrer")
        return
    
    # Utiliser batch processing avec tar archives (sans parallélisme pour éviter conflits HDFS)
    print(f"  [INFO] Utilisation de batch processing avec tar archives (mode séquentiel)...")
    
    import subprocess
    import tarfile
    import tempfile
    
    batch_size = 200  # Taille de batch
    
    # Préparer les batches
    batches = []
    for i in range(0, num_files, batch_size):
        batch_files = txt_files[i:i+batch_size]
        batch_num = i//batch_size + 1
        batches.append((batch_num, batch_files))
    
    total_batches = len(batches)
    print(f"  Traitement de {total_batches} batches de {batch_size} fichiers (séquentiel)")
    
    # Traiter les batches séquentiellement
    total_success = 0
    
    try:
        for batch_num, batch_files in batches:
            print(f"  Migration batch {batch_num}/{total_batches} ({len(batch_files)} fichiers)...")
            
            # Créer un tar archive localement
            temp_tar = tempfile.NamedTemporaryFile(suffix='.tar', delete=False)
            temp_tar_path = temp_tar.name
            temp_tar.close()
            
            try:
                # Créer l'archive tar avec tous les fichiers du batch
                with tarfile.open(temp_tar_path, 'w') as tar:
                    for filepath in batch_files:
                        try:
                            tar.add(filepath, arcname=os.path.basename(filepath))
                        except Exception as e:
                            print(f"    ⚠ Erreur ajout {os.path.basename(filepath)} à l'archive: {e}")
                
                # Copier l'archive vers le conteneur
                copy_result = subprocess.run(
                    ['docker', 'cp', temp_tar_path, 'hadoop-namenode:/tmp/batch.tar'],
                    capture_output=True,
                    timeout=60
                )
                
                if copy_result.returncode != 0:
                    error_msg = copy_result.stderr.decode('utf-8', errors='ignore')[:100]
                    print(f"    ⚠ Erreur copie tar: {error_msg}")
                    os.unlink(temp_tar_path)
                    continue
                
                # Extraire et copier vers HDFS
                hdfs_path_only = f"{HDFS_BASE_DIR}/data/corpus_initial"
                extract_dir = f"/tmp/batch_extract_{batch_num}"
                exec_result = subprocess.run(
                    [
                        'docker', 'exec', 'hadoop-namenode', 'bash', '-c',
                        f'mkdir -p {extract_dir} && '
                        f'tar -xf /tmp/batch.tar -C {extract_dir} && '
                        # Copier fichier par fichier pour éviter les conflits
                        f'for f in {extract_dir}/*.txt; do hdfs dfs -put -f "$f" {hdfs_path_only}/$(basename "$f") || true; done && '
                        f'rm -rf /tmp/batch.tar {extract_dir}'
                    ],
                    capture_output=True,
                    timeout=180
                )
                
                if exec_result.returncode == 0:
                    total_success += len(batch_files)
                    print(f"    ✓ Batch {batch_num}/{total_batches}: {len(batch_files)} fichiers migrés")
                else:
                    error_msg = exec_result.stderr.decode('utf-8', errors='ignore')[:200]
                    print(f"    ⚠ Batch {batch_num}/{total_batches}: Erreur HDFS: {error_msg}")
                
            except subprocess.TimeoutExpired:
                print(f"    ⚠ Batch {batch_num}/{total_batches}: Timeout")
            except Exception as e:
                print(f"    ⚠ Batch {batch_num}/{total_batches}: Erreur - {e}")
            finally:
                # Nettoyer le fichier tar local
                if os.path.exists(temp_tar_path):
                    os.unlink(temp_tar_path)
                    
    except KeyboardInterrupt:
        print("\n[INFO] Migration interrompue par l'utilisateur")
        print(f"  Fichiers migrés jusqu'à présent: {total_success}/{num_files}")
    except Exception as e:
        print(f"\n[ERROR] Erreur lors du traitement: {e}")
    
    print(f"  ✓ Corpus migré: {total_success}/{num_files} fichiers avec succès")

def migrate_idf_model():
    """Migre le modèle IDF vers HDFS"""
    print("\n3. Migration du modèle IDF...")
    
    # In Docker, check mounted path
    if IN_DOCKER:
        docker_model_path = "/home/jovyan/work/storage/idf_model"
        if os.path.exists(docker_model_path):
            model_path = docker_model_path
        else:
            model_path = LOCAL_IDF_MODEL
    else:
        model_path = LOCAL_IDF_MODEL
    
    if not os.path.exists(model_path):
        print(f"  ⚠ Modèle local non trouvé: {model_path}")
        return
    
    try:
        # Lire le modèle depuis le système local
        from pyspark.ml.feature import IDFModel
        # Use file:/// prefix for local paths
        local_path = f"file:///{model_path.replace(os.sep, '/')}"
        idf_model = IDFModel.load(local_path)
        
        # Sauvegarder vers HDFS
        idf_model.write().overwrite().save(HDFS_IDF_MODEL)
        print(f"  ✓ Modèle IDF migré vers {HDFS_IDF_MODEL}")
    except Exception as e:
        print(f"  ❌ Erreur migration modèle IDF: {e}")

def migrate_reference_vectors():
    """Migre les vecteurs de référence vers HDFS"""
    print("\n4. Migration des vecteurs de référence...")
    
    # In Docker, check mounted path
    if IN_DOCKER:
        docker_vectors_path = "/home/jovyan/work/storage/reference_vectors"
        if os.path.exists(docker_vectors_path):
            vectors_path = docker_vectors_path
        else:
            vectors_path = LOCAL_REFERENCE_VECTORS
    else:
        vectors_path = LOCAL_REFERENCE_VECTORS
    
    if not os.path.exists(vectors_path):
        print(f"  ⚠ Vecteurs locaux non trouvés: {vectors_path}")
        return
    
    try:
        # Lire depuis le système local
        local_path = f"file:///{vectors_path.replace(os.sep, '/')}"
        df_vectors = spark.read.parquet(local_path)
        count = df_vectors.count()
        
        # Écrire vers HDFS
        df_vectors.write.mode("overwrite").parquet(HDFS_REFERENCE_VECTORS)
        print(f"  ✓ Vecteurs de référence migrés: {count} documents vers {HDFS_REFERENCE_VECTORS}")
    except Exception as e:
        print(f"  ❌ Erreur migration vecteurs: {e}")

# Exécution de la migration
if __name__ == "__main__":
    import sys
    
    # Check if user wants to migrate models only
    migrate_models = '--migrate-models' in sys.argv
    
    try:
        # Always create directory structure
        create_hdfs_dirs()
        
        if migrate_models:
            # Optionally migrate IDF model and reference vectors
            migrate_idf_model()
            migrate_reference_vectors()
            print("\n=== MIGRATION DES MODÈLES TERMINÉE ===")
        else:
            # Only create directories
            print("\n=== STRUCTURE HDFS CRÉÉE ===")
            print(f"Répertoires disponibles sur HDFS: {HDFS_BASE_URL}{HDFS_BASE_DIR}")
            print("\n[INFO] Pour migrer les fichiers du corpus, utilisez:")
            print("  .\\migrate_fast.ps1")
            print("\n[INFO] Pour migrer les modèles IDF et vecteurs, utilisez:")
            print("  python scripts/0_migrate_to_hdfs.py --migrate-models")
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

