"""
Configuration HDFS pour NetPlag
Centralise les chemins et configurations HDFS
"""

import os

# Detect if running in Docker
IN_DOCKER = os.path.exists('/.dockerenv') or os.getenv('IN_DOCKER', 'false').lower() == 'true'

# Configuration HDFS
# When running in Docker, use internal network URL (namenode hostname)
# When running on host, use localhost
if IN_DOCKER:
    HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "hdfs://namenode:8020")
else:
    HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "hdfs://localhost:8020")
# Chemins HDFS
HDFS_BASE_DIR = "/netplag"
HDFS_DATA_DIR = f"{HDFS_BASE_DIR}/data"
HDFS_STORAGE_DIR = f"{HDFS_BASE_DIR}/storage"

# Chemins complets HDFS
HDFS_CORPUS_INITIAL = f"{HDFS_BASE_URL}{HDFS_DATA_DIR}/corpus_initial"
HDFS_STREAM_INPUT = f"{HDFS_BASE_URL}{HDFS_DATA_DIR}/stream_input"
HDFS_STREAM_SOURCE = f"{HDFS_BASE_URL}{HDFS_DATA_DIR}/stream_source"

HDFS_IDF_MODEL = f"{HDFS_BASE_URL}{HDFS_STORAGE_DIR}/idf_model"
HDFS_REFERENCE_VECTORS = f"{HDFS_BASE_URL}{HDFS_STORAGE_DIR}/reference_vectors"
HDFS_STREAMING_VECTORS = f"{HDFS_BASE_URL}{HDFS_STORAGE_DIR}/streaming_vectors"
HDFS_PLAGIARISM_RESULTS = f"{HDFS_BASE_URL}{HDFS_STORAGE_DIR}/plagiarism_results"
HDFS_REPORTS = f"{HDFS_BASE_URL}{HDFS_STORAGE_DIR}/reports"

# Chemins locaux (pour migration)
# Use os.path to construct paths relative to project root or use environment variable
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_BASE_DIR = os.getenv("NETPLAG_LOCAL_DATA", os.path.join(_project_root, "data"))
LOCAL_STORAGE_DIR = os.getenv("NETPLAG_LOCAL_STORAGE", os.path.join(_project_root, "storage"))

LOCAL_CORPUS_INITIAL = os.path.join(LOCAL_BASE_DIR, "corpus_initial")
LOCAL_STREAM_INPUT = os.path.join(LOCAL_BASE_DIR, "stream_input")
LOCAL_STREAM_SOURCE = os.path.join(LOCAL_BASE_DIR, "stream_source")

LOCAL_IDF_MODEL = os.path.join(LOCAL_STORAGE_DIR, "idf_model")
LOCAL_REFERENCE_VECTORS = os.path.join(LOCAL_STORAGE_DIR, "reference_vectors")
LOCAL_STREAMING_VECTORS = os.path.join(LOCAL_STORAGE_DIR, "streaming_vectors")

# Configuration Spark pour HDFS
def get_spark_hdfs_config():
    """Retourne la configuration Spark pour HDFS"""
    config = {
        "spark.hadoop.fs.defaultFS": HDFS_BASE_URL,
        "spark.hadoop.fs.hdfs.impl": "org.apache.hadoop.hdfs.DistributedFileSystem",
        "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.LocalFileSystem"
    }
    
    # When running on Windows host, force DataNode connections to use hostname (localhost)
    # This works around the issue where DataNode advertises internal Docker IP (172.21.0.3:9866)
    # which Windows host cannot reach. By using hostname (localhost), Docker port mapping handles it.
    if not IN_DOCKER:
        # Configure client to use hostname for DataNode connections (so it uses localhost)
        config["spark.hadoop.dfs.client.use.datanode.hostname"] = "true"
        config["spark.hadoop.dfs.datanode.use.datanode.hostname"] = "true"
        # Increase timeouts for Windows networking
        config["spark.hadoop.dfs.client.socket-timeout"] = "60000"
        config["spark.hadoop.dfs.datanode.socket.write.timeout"] = "60000"
        # Additional settings to help with Windows networking
        config["spark.hadoop.dfs.client.read.shortcircuit"] = "false"
        config["spark.hadoop.dfs.domain.socket.path"] = ""
    
    return config

