import os
import sys
import tempfile
import subprocess
import signal
import atexit

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
else:
    # In Docker, assume Java is properly configured
    security_file_found = True
    temp_security_file = None

if not IN_DOCKER and not security_file_found:
    print(f"[ERROR] Java security file not found at {java_security_file}")
    print("[INFO] This indicates an incomplete Java 17 installation.")
    print("[INFO] Attempting workarounds...")
    
    # Check if security directory exists
    if not os.path.exists(java_security_dir):
        print(f"[ERROR] Security directory missing: {java_security_dir}")
        print("[ERROR] Java installation appears corrupted.")
        print("\n[SOLUTION] Please fix your Java 17 installation:")
        print("  1. Reinstall Java 17 from https://adoptium.net/ or Oracle")
        print("  2. OR manually create the security file (requires admin):")
        print(f"     - Create directory: {java_security_dir}")
        print("     - Copy java.security from a working Java installation")
        print("\n[INFO] Attempting to continue with workarounds...")
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
        print("[WARN] Note: This may not fully resolve the issue as Java expects the file in its installation directory.")
    except Exception as e:
        print(f"[WARN] Could not create security file: {e}")
elif not IN_DOCKER:
    print(f"[OK] Java security file found at {java_security_file}")

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
# Checkpoint location for streaming (use local filesystem, not HDFS)
streaming_checkpoint = os.path.join(temp_dir, "spark_streaming_checkpoint")

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import Spark AFTER all environment variables are set
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, IDFModel
from pyspark.sql.functions import col, udf, input_file_name, broadcast
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, when
import re
import time

# Import HDFS configuration and similarity module
from config.hdfs_config import (
    HDFS_STREAM_INPUT, HDFS_IDF_MODEL, HDFS_REFERENCE_VECTORS,
    HDFS_STREAMING_VECTORS, HDFS_PLAGIARISM_RESULTS,
    HDFS_BASE_URL, get_spark_hdfs_config
)
from scripts.similarity import cosine_similarity_udf, find_plagiarism_candidates

# Configuration
PLAGIARISM_THRESHOLD = 0.7  # Seuil de similarité pour considérer comme plagiat
TOP_K_RESULTS = 10  # Nombre de documents similaires à retourner

# Java security options (only needed on Windows host)
if not IN_DOCKER:
    # Java 17 security options to fix "Error loading java.security file"
    java_home_path = os.environ.get('JAVA_HOME', '').replace('\\', '/')
    
    # Build Java security options
    java_security_options_parts = [
        f"-Djava.home={java_home_path}",
        "-Dfile.encoding=UTF-8",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+UseG1GC",
        "-Djava.awt.headless=true"
    ]
    
    # If security file is missing, try aggressive workarounds
    if not security_file_found:
        java_security_options_parts.extend([
            "-Djava.security.manager=",
            "-Djava.security.policy=",
            "-XX:+DisableAttachMechanism"
        ])
        print("[WARN] Using workaround JVM options (may not fully resolve the issue)")
    
    # If we created a temp security file, try to reference it
    if temp_security_file and os.path.exists(temp_security_file):
        temp_security_path = temp_security_file.replace('\\', '/')
        # Convert Windows path to file:// URL format
        if temp_security_path[1] == ':':  # Windows absolute path like C:/...
            temp_security_path = '/' + temp_security_path.replace(':', '')
        java_security_options_parts.append(f"-Djava.security.properties=file://{temp_security_path}")
        print(f"[INFO] Attempting to use custom security properties: {temp_security_file}")
    
    java_security_options = " ".join(java_security_options_parts)
else:
    # In Docker, use minimal Java options
    java_security_options = "-Dfile.encoding=UTF-8 -Djava.awt.headless=true"

# Get HDFS config first
hdfs_config = get_spark_hdfs_config()

# Stop any existing Spark session to avoid conflicts
try:
    existing_spark = SparkSession.getActiveSession()
    if existing_spark:
        print("[INFO] Stopping existing SparkSession...")
        existing_spark.stop()
        time.sleep(1)  # Give it time to fully stop
except:
    pass

# Build SparkSession using builder pattern (more reliable)
print("[INFO] Creating SparkSession...")
try:
    # Start with basic configuration
    spark_builder = SparkSession.builder \
        .appName("NetPlag_Streaming") \
        .master("local[4]")
    
    # Add core Spark configs
    spark_builder = spark_builder \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.python.worker.memory", "512m")
    
    # Add Java options only if not in Docker
    if not IN_DOCKER:
        spark_builder = spark_builder \
            .config("spark.executorEnv.JAVA_HOME", os.environ.get('JAVA_HOME', '')) \
            .config("spark.driver.extraJavaOptions", java_security_options) \
            .config("spark.executor.extraJavaOptions", java_security_options)
    else:
        # In Docker, add minimal Java options
        spark_builder = spark_builder \
            .config("spark.driver.extraJavaOptions", java_security_options) \
            .config("spark.executor.extraJavaOptions", java_security_options)
    
    spark_builder = spark_builder \
        .config("spark.sql.warehouse.dir", spark_warehouse) \
        .config("spark.local.dir", spark_local) \
        .config("spark.sql.streaming.checkpointLocation", streaming_checkpoint)
    
    # Add HDFS config
    for key, value in hdfs_config.items():
        spark_builder = spark_builder.config(key, value)
    
    # Disable native IO for Windows to fix checkpoint access issue
    if not IN_DOCKER:
        spark_builder = spark_builder \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .config("spark.hadoop.hadoop.tmp.dir", os.path.join(tempfile.gettempdir(), "hadoop_tmp"))
    
    # Create the session
    spark = spark_builder.getOrCreate()
    
    # Verify the session is working by accessing sparkContext
    _ = spark.sparkContext
    print("[OK] SparkSession created successfully!")
    
except Exception as e:
    print(f"[ERROR] Failed to create SparkSession: {e}")
    import traceback
    traceback.print_exc()
    # Try to stop any existing session
    try:
        spark = SparkSession.getActiveSession()
        if spark:
            spark.stop()
    except:
        pass
    raise

# MANDATORY: Verify HDFS connection before proceeding
print("[INFO] Verifying HDFS connection (MANDATORY)...")
try:
    # Get HDFS filesystem
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    
    # Workaround for Windows: Force DataNode connections to use hostname (localhost)
    # The DataNode advertises internal Docker IP (172.21.0.3:9866) which Windows can't reach
    # By using hostname, clients will connect to localhost:9866 which Docker port mapping handles
    if not IN_DOCKER:
        hadoop_conf.set("dfs.client.use.datanode.hostname", "true")
        hadoop_conf.set("dfs.datanode.use.datanode.hostname", "true")
        # Force DataNode to advertise hostname that resolves to localhost
        hadoop_conf.set("dfs.datanode.hostname", "datanode")
        # Increase timeouts for Windows networking
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
    # Try to access root directory to verify connection
    root_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path("/")
    fs.exists(root_path)
    print(f"[OK] HDFS connection verified: {HDFS_BASE_URL}")
    
    if not IN_DOCKER:
        print("[INFO] Running on Windows host - DataNode IP workaround enabled")
except Exception as e:
    print(f"\n[ERROR] HDFS connection failed: {e}")
    print(f"\n{'='*60}")
    print("HDFS CONNECTION IS MANDATORY FOR THIS SCRIPT")
    print(f"{'='*60}")
    print(f"\nHDFS URL: {HDFS_BASE_URL}")
    print("\nTo start HDFS, run:")
    print("  docker-compose up -d")
    print("\nOr check if HDFS is running:")
    print("  docker ps | findstr hadoop")
    print("\nVerify HDFS is accessible:")
    print("  docker exec hadoop-namenode hdfs dfsadmin -report")
    print(f"\n{'='*60}\n")
    spark.stop()
    sys.exit(1)

spark.sparkContext.setLogLevel("ERROR")

print("--- DÉMARRAGE DU STREAMING (HDFS + PLAGIAT) ---")

# Global variable to store streaming query for cleanup
streaming_query = None

# 1. Load reference database and IDF model from HDFS
print("Loading reference database and IDF model from HDFS...")
try:
    # Load existing reference vectors from HDFS
    df_reference = spark.read.parquet(HDFS_REFERENCE_VECTORS)
    num_refs = df_reference.count()
    if num_refs == 0:
        print("\n[ERROR] No reference vectors found in HDFS!")
        print(f"Expected reference vectors at: {HDFS_REFERENCE_VECTORS}")
        print("\n[INFO] Make sure you've run Step 1 (batch initialization) first:")
        print("  python scripts/1_batch_init.py")
        spark.stop()
        sys.exit(1)
    print(f"✓ Loaded {num_refs} reference documents from HDFS")
    
    # Load IDF model from HDFS
    try:
        idf_model = IDFModel.load(HDFS_IDF_MODEL)
        print(f"✓ Loaded IDF model from {HDFS_IDF_MODEL}")
    except Exception as e:
        print(f"⚠ Warning: Could not load IDF model from HDFS: {e}")
        print("  Will compute TF features only (without IDF)")
        idf_model = None
    
    # Initialize TF transformer
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=5000)
    
except Exception as e:
    print(f"❌ ERROR loading reference data: {e}")
    import traceback
    traceback.print_exc()
    spark.stop()
    sys.exit(1)

# 2. Define streaming source from HDFS
print(f"Setting up streaming from: {HDFS_STREAM_INPUT}")

# Check if stream input directory exists
try:
    stream_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(HDFS_STREAM_INPUT.replace(HDFS_BASE_URL, ""))
    if not fs.exists(stream_path):
        print(f"\n[WARN] Stream input directory does not exist: {HDFS_STREAM_INPUT}")
        print("[INFO] Creating directory...")
        fs.mkdirs(stream_path)
        print("[OK] Directory created. Add .txt files to this directory for processing.")
except Exception as e:
    print(f"[WARN] Could not verify stream input directory: {e}")

df_stream = spark.readStream \
    .format("text") \
    .option("wholetext", "true") \
    .option("maxFilesPerTrigger", 10) \
    .load(f"{HDFS_STREAM_INPUT}/*.txt") \
    .withColumn("filename", input_file_name())

# 3. Text preprocessing (same as batch)
def clean_text(text):
    if not text: 
        return []
    text = text.lower()[:50000]
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    words = text.split()
    words = [w for w in words if len(w) > 2]
    return words

clean_udf = udf(clean_text, ArrayType(StringType()))
df_clean = df_stream.withColumn("words", clean_udf(col("value")))

# 4. Process each micro-batch with plagiarism detection
def process_batch(df_batch, batch_id):
    # Always log batch processing for debugging
    file_count = df_batch.count()
    print(f"\n--- Processing Batch ID: {batch_id} ---")
    print(f"[DEBUG] Files detected in this batch: {file_count}")
    
    if file_count == 0: 
        print("[DEBUG] No files to process in this batch")
        return
    
    print(f"\n--- Processing Batch ID: {batch_id} ---")
    
    try:
        # Apply TF hashing
        tf_batch = hashingTF.transform(df_batch)
        
        # Apply IDF transform if model is available
        if idf_model is not None:
            df_vectors = idf_model.transform(tf_batch).select("filename", col("features").alias("features"))
            print("✓ Applied TF-IDF transformation")
        else:
            df_vectors = tf_batch.select("filename", col("rawFeatures").alias("features"))
            print("⚠ Using TF features only (IDF model not available)")
        
        count = df_vectors.count()
        print(f"✓ Documents received and vectorized: {count}")
        df_vectors.select("filename").show(count, truncate=False)
        
        # 5. Calculate similarity with reference corpus
        print("Calculating similarity with reference corpus...")
        try:
            # Use similarity module to find plagiarism candidates
            # Note: find_plagiarism_candidates returns columns: document_filename, reference_filename, similarity_score, is_plagiarism
            plagiarism_results = find_plagiarism_candidates(
                df_vectors, df_reference,
                threshold=PLAGIARISM_THRESHOLD,
                top_k=TOP_K_RESULTS,
                vec_col="features",
                id_col="filename"
            )
            
            # Show results
            plagiarism_count = plagiarism_results.filter(col("is_plagiarism") == True).count()
            print(f"✓ Plagiarism analysis complete: {plagiarism_count} potential plagiarism cases found")
            
            if plagiarism_count > 0:
                print("\nTop plagiarism matches:")
                plagiarism_results.filter(col("is_plagiarism") == True) \
                    .orderBy(col("similarity_score").desc()) \
                    .show(20, truncate=False)
            
            # Save plagiarism results to HDFS
            plagiarism_results.write \
                .mode("append") \
                .parquet(HDFS_PLAGIARISM_RESULTS)
            print(f"✓ Plagiarism results saved to HDFS")
            
        except Exception as e:
            print(f"⚠ Error in plagiarism detection: {e}")
            import traceback
            traceback.print_exc()
        
        # 6. Append vectors to streaming database on HDFS
        df_vectors.write \
            .mode("append") \
            .parquet(HDFS_STREAMING_VECTORS)
        
        print(f"✓ Added {count} document(s) to streaming database on HDFS")
        
    except Exception as e:
        print(f"⚠ Error processing batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# Cleanup function for graceful shutdown
def cleanup():
    """Cleanup function to stop streaming query and Spark session"""
    global streaming_query
    if streaming_query is not None:
        try:
            print("\n[INFO] Stopping streaming query...")
            streaming_query.stop()
            print("[OK] Streaming query stopped")
        except Exception as e:
            print(f"[WARN] Error stopping streaming query: {e}")
    
    try:
        print("[INFO] Stopping SparkSession...")
        spark.stop()
        print("[OK] SparkSession stopped")
    except Exception as e:
        print(f"[WARN] Error stopping SparkSession: {e}")

# Register cleanup function
atexit.register(cleanup)

# Handle Ctrl+C gracefully
def signal_handler(sig, frame):
    print("\n\n[INFO] Interrupt received, shutting down gracefully...")
    cleanup()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
if hasattr(signal, 'SIGTERM'):
    signal.signal(signal.SIGTERM, signal_handler)

# 5. Start streaming
print("\n🔴 STREAMING ACTIVE - Watching for new files on HDFS...")
print(f"Monitoring: {HDFS_STREAM_INPUT}")
print(f"Plagiarism threshold: {PLAGIARISM_THRESHOLD}")
print(f"Top-K results: {TOP_K_RESULTS}")
print("Press Ctrl+C to stop\n")

try:
    # Use HDFS for checkpoint to avoid Windows native IO issues with local filesystem
    # This is more reliable on Windows and avoids UnsatisfiedLinkError
    checkpoint_uri = f"{HDFS_BASE_URL}/netplag/storage/streaming_checkpoint"
    
    # Create checkpoint directory on HDFS if it doesn't exist
    try:
        checkpoint_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path("/netplag/storage/streaming_checkpoint")
        if not fs.exists(checkpoint_path):
            fs.mkdirs(checkpoint_path)
            print(f"[INFO] Created HDFS checkpoint directory: {checkpoint_uri}")
        else:
            print(f"[INFO] Using existing HDFS checkpoint directory: {checkpoint_uri}")
    except Exception as e:
        print(f"[WARN] Could not verify/create HDFS checkpoint directory: {e}")
        print(f"[INFO] Will attempt to use: {checkpoint_uri}")
    
    print(f"[INFO] Using HDFS checkpoint location: {checkpoint_uri}")
    
    streaming_query = df_clean.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='5 seconds') \
        .option("checkpointLocation", checkpoint_uri) \
        .start()
    
    streaming_query.awaitTermination()
except KeyboardInterrupt:
    print("\n[INFO] Keyboard interrupt received")
    cleanup()
except Exception as e:
    print(f"\n[ERROR] Streaming error: {e}")
    import traceback
    traceback.print_exc()
    cleanup()
    sys.exit(1)
