# NetPlag - Complete Technical Analysis for Report

## Executive Summary

**NetPlag** is a distributed plagiarism detection system designed to process and analyze large volumes of academic documents using big data technologies. The system employs **TF-IDF vectorization** and **cosine similarity** algorithms to identify potential plagiarism cases in both batch and real-time streaming modes. Built on **Apache Spark** and **Hadoop HDFS**, it provides scalable storage, distributed processing, and real-time analytics through **Elasticsearch** integration and an interactive web dashboard.

---

## 1. System Architecture & Technology Stack

### Core Technologies
- **Apache Spark 3.5.7**: Distributed data processing engine for batch and streaming operations
- **Hadoop HDFS**: Distributed file system for storing documents, models, and results
- **Elasticsearch 8.11.0**: Search engine for indexing and querying plagiarism results
- **Flask 2.3+**: Web framework powering the interactive dashboard
- **Python 3.11**: Primary programming language
- **Docker**: Containerization platform for HDFS and Elasticsearch services

### Infrastructure Components
- **NameNode & DataNode**: HDFS cluster for distributed storage (Docker containers)
- **Dashboard Service**: Flask-based web application (containerized)
- **Elasticsearch Service**: Search and analytics engine (containerized)
- **Spark Processing**: Runs on Windows host or Docker for distributed computing

---

## 2. Data Processing Pipeline

### **Step 0: Infrastructure Setup**
**Purpose**: Establish HDFS directory structure and prepare environment

**Key Scripts**:
- `0_migrate_to_hdfs.py`: Creates HDFS directory structure with proper permissions
- `migrate_fast.ps1`: Ultra-fast file migration using Docker volume mounts and tar archives

**Directory Structure Created**:
```
/netplag/
├── data/
│   ├── corpus_initial/      # Reference corpus
│   ├── stream_input/         # Incoming documents for streaming
│   └── stream_source/        # Source files for simulation
└── storage/
    ├── idf_model/            # TF-IDF model
    ├── reference_vectors/    # Vectorized reference corpus
    ├── streaming_vectors/    # Vectorized streaming documents
    ├── plagiarism_results/   # Detection results
    └── reports/              # Analysis reports
```

**Technical Features**:
- Docker exec-based directory creation to avoid Windows permission issues
- Batch processing (500 files per batch) using tar archives for efficient migration
- Automatic HDFS connection verification before operations

---

### **Step 1: Batch Initialization**
**Script**: `1_batch_init.py`

**Purpose**: Build reference corpus with TF-IDF features

**Process Flow**:
1. **Document Reading**: Load `.txt` files from HDFS corpus directory
2. **Text Preprocessing**:
   - Convert to lowercase
   - Remove special characters (keep only alphanumeric)
   - Filter words shorter than 3 characters
   - Truncate to 50,000 characters per document
3. **Feature Extraction**:
   - **HashingTF**: Convert text to term frequency vectors (5,000 features)
   - **IDF**: Calculate inverse document frequency to weight terms
4. **Model Persistence**:
   - Save IDF model to HDFS (`/netplag/storage/idf_model`)
   - Save reference vectors to HDFS (`/netplag/storage/reference_vectors`)

**Technical Implementation**:
- Uses Spark DataFrame API for distributed processing
- Implements custom UDF (User Defined Function) for text cleaning
- Employs Parquet format for efficient columnar storage
- Handles Java security configuration for Windows compatibility

---

### **Step 2: Streaming Processing**
**Script**: `2_streaming_app.py`

**Purpose**: Real-time plagiarism detection for incoming documents

**Process Flow**:
1. **Stream Monitoring**: Watch HDFS `stream_input` directory for new `.txt` files
2. **Micro-Batch Processing**:
   - Trigger every 5 seconds
   - Process up to 10 files per batch
3. **Vectorization**: Apply TF-IDF transformation using pre-trained model
4. **Similarity Calculation**: Compare against reference corpus using cosine similarity
5. **Result Storage**:
   - Save streaming vectors to HDFS
   - Store plagiarism results with scores

**Technical Features**:
- **Spark Structured Streaming** with checkpoint-based fault tolerance
- HDFS-based checkpointing to avoid Windows native IO issues
- Broadcast join optimization for reference vectors
- Custom similarity computation using SparseVector operations
- Configurable plagiarism threshold (default: 0.7)

**Key Algorithm - Cosine Similarity**:
```
similarity = (v1 · v2) / (||v1|| × ||v2||)
```
Where v1 and v2 are TF-IDF vectors of compared documents.

---

### **Step 3: Batch Analysis**
**Script**: `4_plagiarism_analysis.py`

**Purpose**: Comprehensive analysis of all streaming documents

**Process Flow**:
1. **Data Loading**:
   - Load streaming vectors from HDFS
   - Load reference vectors from HDFS
2. **Similarity Matrix Computation**:
   - Cross join: streaming docs × reference docs
   - Calculate cosine similarity for each pair
   - Filter by plagiarism threshold
3. **Statistical Analysis**:
   - Total plagiarism cases detected
   - Maximum similarity score
   - Average similarity score
   - Per-document summaries
4. **Report Generation**:
   - Detailed results (Parquet format)
   - Summary statistics (Parquet format)
   - JSON export for easy reading

**Output Files on HDFS**:
- `/netplag/storage/reports/detailed_results`: Full similarity matrix
- `/netplag/storage/reports/summary`: Per-document statistics
- `/netplag/storage/reports/plagiarism_cases.json`: Human-readable report

---

### **Step 4: Elasticsearch Indexing**
**Script**: `6_elasticsearch_indexer.py`

**Purpose**: Index results for fast search and visualization

**Elasticsearch Indices Created**:

1. **`plagiarism_reports`**: Detailed detection results
   - `document_filename`: Source document
   - `reference_filename`: Matched reference
   - `similarity_score`: Cosine similarity (0-1)
   - `is_plagiarism`: Boolean flag (threshold-based)
   - `timestamp`: Analysis timestamp

2. **`analysis_results`**: Document-level summaries
   - `document_filename`: Document name
   - `num_matches`: Count of plagiarism matches
   - `max_score`: Highest similarity score
   - `avg_score`: Average similarity score
   - `analysis_timestamp`: Analysis time

3. **`documents`**: Document metadata (reserved for future use)

**Technical Implementation**:
- Bulk indexing for performance (1,000 docs per batch)
- Automatic index creation with proper mappings
- Connection retry logic for Docker environment
- Index refresh for immediate searchability

---

### **Step 5: Web Dashboard**
**Script**: `7_dashboard.py`
**Template**: `templates/dashboard.html`

**Purpose**: Interactive visualization of plagiarism detection results

**Features**:

1. **Real-Time Statistics**:
   - Total cases analyzed
   - Confirmed plagiarism count
   - High similarity cases (>0.8)
   - Average/Max/Min similarity scores

2. **Interactive Visualizations**:
   - **Similarity Distribution Histogram**: Shows distribution of similarity scores in 0.1 intervals
   - **Top Documents Chart**: Bar chart of documents with most matches
   - **Analysis Summary Table**: Document-level statistics with sorting

3. **Search & Filter**:
   - Search by document or reference filename
   - Filter by minimum similarity score
   - Pagination support (20 results per page)

4. **RESTful API Endpoints**:
   - `/api/stats`: Overall statistics
   - `/api/plagiarism_cases`: Paginated case list
   - `/api/similarity_distribution`: Histogram data
   - `/api/top_documents`: Most matched documents
   - `/api/analysis_summary`: Summary table data
   - `/api/search`: Search functionality

**Technical Stack**:
- **Backend**: Flask with Elasticsearch client
- **Frontend**: HTML5, CSS3, JavaScript, Chart.js
- **Deployment**: Docker container (Python 3.11-slim)
- Auto-refresh every 30 seconds
- Responsive design for all screen sizes

---

### **Step 6: Full Stream Process (Integrated Pipeline)**
**Script**: `8_full_streamprocess.py`

**Purpose**: All-in-one automated pipeline combining Steps 2, 3, and 4

**Workflow**:
1. **File Detection**: Monitor HDFS `stream_input` directory
2. **Streaming Processing**: Apply TF-IDF and detect plagiarism (Step 2)
3. **Automatic Batch Analysis**: Analyze all streaming vectors after each batch (Step 3)
4. **Automatic Indexing**: Index results to Elasticsearch immediately (Step 4)

**Key Advantages**:
- **Fully Automated**: No manual intervention required
- **Real-Time Results**: Immediate availability in dashboard
- **Production-Ready**: Complete pipeline for deployment
- **Efficient**: Single process handles entire workflow

**Configuration**:
- Batch size: 10 files per trigger
- Trigger interval: 5 seconds
- Plagiarism threshold: 0.7
- Top-K results: 10

---

## 3. Technical Implementation Details

### Text Processing & Vectorization

**Preprocessing Pipeline**:
1. Text normalization (lowercase conversion)
2. Special character removal (regex: `[^a-z0-9\s]`)
3. Tokenization (split by whitespace)
4. Short word filtering (min length: 3 characters)
5. Document truncation (max: 50,000 characters)

**TF-IDF Feature Extraction**:
- **Feature Space**: 5,000 dimensions (HashingTF)
- **IDF Min Document Frequency**: 2 (reduces noise)
- **Vector Format**: SparseVector (memory-efficient)

### Similarity Computation

**Cosine Similarity Formula**:
```python
def cosine_similarity(v1, v2):
    dot_product = np.dot(v1.toArray(), v2.toArray())
    norm1 = np.linalg.norm(v1.toArray())
    norm2 = np.linalg.norm(v2.toArray())
    return dot_product / (norm1 * norm2) if norm1 > 0 and norm2 > 0 else 0.0
```

**Implementation Details**:
- Custom UDF for Spark DataFrame operations
- Supports both SparseVector and DenseVector
- Broadcast optimization for reference corpus
- Window functions for top-K selection

### HDFS Configuration & Windows Compatibility

**Key Workarounds**:
1. **DataNode Hostname Resolution**:
   - Force `dfs.client.use.datanode.hostname=true`
   - Maps DataNode internal IP to `localhost:9866`
   
2. **Checkpoint Storage**:
   - Use HDFS checkpoints instead of local filesystem
   - Avoids Windows native IO library issues
   
3. **Java Security File Issues**:
   - Create temporary security file in temp directory
   - Pass custom Java options to Spark executors

**Network Configuration**:
```yaml
# docker-compose.yml
services:
  namenode:
    ports:
      - "8020:8020"  # RPC
      - "9870:9870"  # Web UI
  datanode:
    ports:
      - "9864:9864"  # Web UI
      - "9866:9866"  # Data transfer
```

---

## 4. Docker Infrastructure

### Service Definitions

1. **NameNode**:
   - Image: `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`
   - Persistent volume: `hadoop_namenode`
   - Health check: HDFS admin report
   - Memory: 2GB limit, 1GB reserved

2. **DataNode**:
   - Image: `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8`
   - Persistent volume: `hadoop_datanode`
   - Depends on NameNode health
   - Memory: 2GB limit, 1GB reserved

3. **Elasticsearch**:
   - Image: `docker.elastic.co/elasticsearch/elasticsearch:8.11.0`
   - Single-node cluster, security disabled
   - Persistent volume: `elasticsearch_data`
   - Memory: 512MB JVM heap

4. **Dashboard**:
   - Custom image from `Dockerfile.dashboard`
   - Based on Python 3.11-slim
   - Auto-restart on failure
   - Health check: `/api/stats` endpoint

### Volume Management

**Persistent Volumes**:
- `hadoop_namenode`: HDFS metadata
- `hadoop_datanode`: HDFS data blocks
- `elasticsearch_data`: Elasticsearch indices

**Backup Strategy**:
- `backup_docker.ps1`: Container and volume backup
- `backup_hdfs_data.ps1`: HDFS data export
- `restore_docker.ps1`: Full restoration

---

## 5. Configuration Management

### HDFS Configuration (`config/hdfs_config.py`)

**Environment Detection**:
```python
IN_DOCKER = os.path.exists('/.dockerenv')
HDFS_BASE_URL = "hdfs://namenode:8020" if IN_DOCKER else "hdfs://localhost:8020"
```

**Key Paths**:
- Base: `/netplag/`
- Corpus: `/netplag/data/corpus_initial/`
- Stream input: `/netplag/data/stream_input/`
- IDF model: `/netplag/storage/idf_model/`
- Reference vectors: `/netplag/storage/reference_vectors/`
- Reports: `/netplag/storage/reports/`

### Elasticsearch Configuration (`config/elasticsearch_config.py`)

**Connection Settings**:
```python
ES_URL = "http://elasticsearch:9200" if IN_DOCKER else "http://localhost:9200"
```

**Index Mappings**:
- Dynamic field detection disabled
- Explicit type definitions for all fields
- Optimized for aggregations and range queries

---

## 6. Utility Modules

### Similarity Module (`scripts/similarity.py`)

**Functions**:
1. `cosine_similarity_sparse(v1, v2)`: Core similarity calculation
2. `cosine_similarity_udf()`: Spark UDF wrapper
3. `compute_similarity_matrix(df1, df2, ...)`: Batch similarity computation
4. `find_plagiarism_candidates(...)`: Complete plagiarism detection workflow

**Features**:
- Handles both SparseVector and DenseVector
- Window-based top-K selection
- Configurable threshold and result count
- Optimized cross-join with broadcast

### Standalone Checker (`scripts/5_standalone_plagiarism_check.py`)

**Capabilities**:
- Single document analysis
- Batch document analysis
- ES-ready JSON export format
- Independent of streaming pipeline

**Use Cases**:
- Ad-hoc document checking
- Testing and validation
- Manual analysis workflows

---

## 7. Operational Workflows

### Initial Setup
```powershell
# 1. Start services
docker-compose up -d

# 2. Create HDFS structure
python scripts/0_migrate_to_hdfs.py

# 3. Migrate corpus
.\migrate_fast.ps1

# 4. Initialize reference corpus
python scripts/1_batch_init.py
```

### Streaming Mode
```powershell
# Option 1: Manual pipeline
python scripts/2_streaming_app.py          # Stream processing
python scripts/4_plagiarism_analysis.py    # Batch analysis
python scripts/6_elasticsearch_indexer.py  # Indexing

# Option 2: Automated pipeline (recommended)
python scripts/8_full_streamprocess.py     # All-in-one
```

### Dashboard Access
```powershell
# Docker deployment
docker-compose up -d dashboard
# Access: http://localhost:5000

# Local deployment
python scripts/7_dashboard.py
# Access: http://localhost:5000
```

---

## 8. Performance Characteristics

### Scalability
- **Batch Processing**: Handles 500+ documents efficiently
- **Streaming**: Real-time processing with 5-second latency
- **Elasticsearch**: Sub-second query response times
- **Dashboard**: Auto-refresh maintains responsiveness

### Optimization Techniques
- **Broadcast Joins**: Reference corpus broadcast to all workers
- **Parquet Storage**: Columnar format for 10x compression
- **SparseVector**: Memory-efficient representation
- **Bulk Indexing**: 1,000 documents per Elasticsearch batch
- **Checkpointing**: Fault-tolerant streaming with exactly-once semantics

---

## 9. System Requirements

### Software Dependencies
- **Java 17**: Required for Spark/Hadoop
- **Python 3.11**: With PySpark, NumPy, Pandas, Flask, Elasticsearch client
- **Docker Desktop**: For HDFS and Elasticsearch containers
- **Windows 10/11** or **Linux**: Tested platforms

### Hardware Recommendations
- **CPU**: 4+ cores for parallel processing
- **RAM**: 8GB minimum (16GB recommended)
- **Storage**: 50GB+ for corpus and results
- **Network**: Gigabit for Docker networking

---

## 10. Key Features & Innovations

### Technical Achievements
1. **Hybrid Architecture**: Windows host + Docker containers
2. **Windows Compatibility**: Comprehensive workarounds for HDFS networking
3. **Fault Tolerance**: Checkpoint-based recovery for streaming
4. **Real-Time Analytics**: Streaming + batch analysis + instant indexing
5. **Full Automation**: Single-command deployment with `8_full_streamprocess.py`

### User Experience
1. **Visual Dashboard**: Chart.js-powered interactive visualizations
2. **RESTful API**: Programmatic access to all functionality
3. **Search & Filter**: Advanced query capabilities via Elasticsearch
4. **Auto-Refresh**: Real-time updates without manual intervention

---

## 11. Data Flow Diagram

```
┌─────────────────┐
│  Corpus Files   │
│   (HDFS)        │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  Step 1: Batch Init     │
│  - TF-IDF Training      │
│  - Reference Vectors    │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  IDF Model + Ref Vectors│
│  (Stored in HDFS)       │
└─────────────────────────┘
         │
         │    ┌──────────────────┐
         │    │ Stream Input     │
         │    │ (New Documents)  │
         │    └────────┬─────────┘
         │             │
         ▼             ▼
┌─────────────────────────────────┐
│  Step 2: Streaming Processing   │
│  - Apply TF-IDF Model           │
│  - Compute Similarity           │
│  - Detect Plagiarism            │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Step 3: Batch Analysis         │
│  - Aggregate Results            │
│  - Generate Reports             │
│  - Calculate Statistics         │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Step 4: Elasticsearch Indexing │
│  - Index Plagiarism Reports     │
│  - Index Analysis Summaries     │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Step 5: Web Dashboard          │
│  - Visualizations               │
│  - Search & Filter              │
│  - Real-time Statistics         │
└─────────────────────────────────┘
```

---

## 12. Algorithm Details

### TF-IDF Vectorization

**Term Frequency (TF)**:
```
TF(t, d) = (Number of times term t appears in document d) / (Total terms in document d)
```

**Inverse Document Frequency (IDF)**:
```
IDF(t, D) = log((Total documents in corpus D) / (Documents containing term t))
```

**TF-IDF Weight**:
```
TF-IDF(t, d, D) = TF(t, d) × IDF(t, D)
```

**Implementation**:
- Uses HashingTF for efficiency (5,000 hash buckets)
- IDF model trained on reference corpus
- Resulting vectors are sparse (typically <1% non-zero values)

### Plagiarism Detection Algorithm

**Similarity Threshold**: 0.7 (configurable)

**Detection Steps**:
1. For each new document D:
   - Vectorize using trained IDF model
2. For each reference document R:
   - Compute cosine_similarity(D, R)
   - If similarity > threshold: Flag as potential plagiarism
3. Return top-K most similar documents

**Complexity**:
- Time: O(N × M × F) where N = streaming docs, M = reference docs, F = features
- Space: O(N × F + M × F) for sparse vectors

---

## 13. Error Handling & Monitoring

### Checkpoint Recovery
- **Location**: HDFS `/netplag/checkpoints/`
- **Frequency**: Every micro-batch (5 seconds)
- **Recovery**: Automatic on restart, continues from last checkpoint

### HDFS Connection Failures
- **Detection**: Health checks on NameNode and DataNode
- **Recovery**: Docker restart policies (automatic restart)
- **Fallback**: Backup and restore scripts available

### Elasticsearch Indexing Failures
- **Retry Logic**: 3 attempts with exponential backoff
- **Batch Processing**: Continues with next batch on persistent failures
- **Logging**: Detailed error messages to console

---

## 14. Security Considerations

### Current Implementation
- **Elasticsearch**: Security disabled (development mode)
- **HDFS**: Basic file permissions, no Kerberos
- **Dashboard**: No authentication (intended for local use)

### Production Recommendations
1. Enable Elasticsearch security (TLS, authentication)
2. Configure HDFS with Kerberos authentication
3. Add dashboard authentication (OAuth, LDAP)
4. Implement network isolation (VPC, firewalls)
5. Enable HDFS encryption at rest
6. Add audit logging for all operations

---

## 15. Future Enhancements

### Potential Improvements
1. **Machine Learning**:
   - Deep learning models (BERT, transformers) for semantic similarity
   - Paraphrase detection using neural networks
   - Automatic threshold optimization

2. **Scalability**:
   - Multi-node Spark cluster for larger corpora
   - Distributed Elasticsearch cluster
   - Horizontal scaling with Kubernetes

3. **Features**:
   - Document upload interface in dashboard
   - Email notifications for plagiarism detection
   - Citation analysis and exclusion
   - Multi-language support

4. **Performance**:
   - GPU acceleration for similarity computation
   - Incremental IDF model updates
   - Caching frequently accessed reference vectors

---

## 16. Conclusion

NetPlag represents a **complete big data solution** for plagiarism detection, demonstrating:
- **Distributed computing** with Apache Spark
- **Scalable storage** with Hadoop HDFS
- **Real-time processing** with Structured Streaming
- **Advanced search** with Elasticsearch
- **Modern web interfaces** with Flask and Chart.js

The system successfully bridges **Windows development environments** with **Linux-based big data tools** through Docker containerization and comprehensive compatibility layers. The modular architecture allows both **step-by-step execution** for learning and **automated pipelines** for production deployment.

**Production-Ready Features**:
✓ Fault-tolerant streaming  
✓ Persistent storage with HDFS  
✓ Searchable results via Elasticsearch  
✓ Visual analytics dashboard  
✓ Comprehensive backup/restore  
✓ Docker-based deployment  

This system can scale from **academic research projects** to **enterprise-grade plagiarism detection** with minimal architectural changes.

---

## 17. Appendix: File Structure Summary

### Configuration Files
- `config/hdfs_config.py`: HDFS paths and Spark configuration
- `config/elasticsearch_config.py`: ES connection and index mappings
- `hadoop-config/hdfs-site.xml`: HDFS cluster configuration
- `docker-compose.yml`: Multi-container orchestration
- `requirements.txt`: Python dependencies

### Processing Scripts
- `scripts/0_migrate_to_hdfs.py`: HDFS setup
- `scripts/1_batch_init.py`: Reference corpus initialization
- `scripts/2_streaming_app.py`: Real-time processing
- `scripts/3_simulateur.py`: Stream simulation tool
- `scripts/4_plagiarism_analysis.py`: Batch analysis
- `scripts/5_standalone_plagiarism_check.py`: Manual checker
- `scripts/6_elasticsearch_indexer.py`: ES integration
- `scripts/7_dashboard.py`: Web dashboard
- `scripts/8_full_streamprocess.py`: Complete pipeline

### Utility Scripts
- `scripts/similarity.py`: Similarity computation module
- `migrate_fast.ps1`: Fast HDFS migration
- `backup_docker.ps1`: Container backup
- `backup_hdfs_data.ps1`: Data backup
- `restore_docker.ps1`: System restoration
- `run_spark_docker.ps1`: Docker Spark execution

### Templates & Documentation
- `templates/dashboard.html`: Dashboard UI
- `PROJECT_SUMMARY.md`: Quick reference
- `SETUP_TRACKING.md`: Setup progress tracker
- `DOCKER_SPARK_README.md`: Docker deployment guide