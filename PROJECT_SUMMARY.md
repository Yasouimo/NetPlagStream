# NetPlag - Project Summary
## Plagiarism Detection System

---

## Project Overview

NetPlag is a distributed plagiarism detection system that uses:
- Apache Spark for distributed processing
- HDFS (Hadoop Distributed File System) for data storage
- Elasticsearch for search and analytics
- TF-IDF vectorization and cosine similarity for plagiarism detection

The system processes documents in batch and streaming modes, comparing them against a reference corpus to detect potential plagiarism cases.

---

## Processing Pipeline

### STEP 0: HDFS Setup
- Create HDFS directory structure
- Set proper permissions
- Script: `scripts/0_migrate_to_hdfs.py`

### STEP 0b: Data Migration
- Migrate corpus files from local to HDFS
- Fast batch processing with tar archives
- Script: `migrate_fast.ps1`

### STEP 1: Batch Initialization
- Read corpus files from HDFS
- Compute TF-IDF features (5000 dimensions)
- Create IDF model
- Generate reference vectors
- Save models to HDFS
- Script: `scripts/1_batch_init.py`

### STEP 2: Streaming Processing
- Monitor HDFS stream_input directory
- Process new files in micro-batches (10 files, 5-second intervals)
- Real-time plagiarism detection
- Save streaming vectors and results to HDFS
- Script: `scripts/2_streaming_app.py`

### STEP 3: Batch Analysis
- Load streaming and reference vectors from HDFS
- Calculate similarity for all document pairs
- Generate comprehensive statistics and reports
- Save results in Parquet and JSON formats
- Script: `scripts/4_plagiarism_analysis.py`

### STEP 5: Elasticsearch Indexing
- Index plagiarism results from HDFS to Elasticsearch
- Enable fast search and filtering
- Create searchable indices
- Script: `scripts/6_elasticsearch_indexer.py`

### STEP 6: Dashboard
- Web-based visualization dashboard
- Interactive charts and statistics
- Real-time search and filtering
- Beautiful, modern UI
- Script: `scripts/7_dashboard.py`

### STEP 7: Full Stream Process (RECOMMENDED)
- Complete end-to-end streaming pipeline
- Combines Steps 2, 3, and 5 automatically
- Detects new files → Processes → Analyzes → Indexes
- All-in-one automated solution
- Script: `scripts/8_full_streamprocess.py`

---

## File Descriptions

### Configuration Files

#### `config/hdfs_config.py`
- HDFS connection configuration
- Auto-detects Docker vs Windows host environment
- Defines HDFS paths and Spark HDFS settings
- Configures DataNode hostname workarounds for Windows

#### `config/elasticsearch_config.py`
- Elasticsearch connection settings
- Index mappings and settings
- Defines three indices: plagiarism_reports, analysis_results, documents

#### `docker-compose.yml`
- Docker services configuration
- HDFS NameNode and DataNode containers
- Elasticsearch container
- Dashboard container
- Network and volume definitions

#### `Dockerfile.dashboard`
- Dockerfile for building dashboard container
- Based on Python 3.11-slim
- Installs dependencies and copies application files

#### `hadoop-config/hdfs-site.xml`
- HDFS DataNode configuration
- Forces DataNode to bind to all interfaces (0.0.0.0)
- Configures hostname usage for Windows compatibility

#### `requirements.txt`
- Python dependencies
- `pyspark==3.5.7`
- `numpy`, `pandas`
- `elasticsearch>=8.11.0,<9.0.0`
- `flask>=2.3.0`

### Main Processing Scripts

#### `scripts/0_migrate_to_hdfs.py`
- Creates HDFS directory structure
- Sets permissions via Docker exec
- Optional: migrates IDF models and reference vectors

#### `scripts/1_batch_init.py`
- Batch initialization script
- Reads corpus from HDFS
- Computes TF-IDF features and IDF model
- Generates reference vectors
- Saves models to HDFS storage

#### `scripts/2_streaming_app.py`
- Real-time streaming application
- Monitors HDFS stream_input directory
- Processes files in micro-batches
- Performs plagiarism detection using cosine similarity
- Saves streaming vectors and results to HDFS

#### `scripts/4_plagiarism_analysis.py`
- Batch plagiarism analysis
- Loads streaming and reference vectors
- Calculates similarity matrix
- Generates statistics and detailed reports
- Saves results in multiple formats (Parquet, JSON)

#### `scripts/5_standalone_plagiarism_check.py`
- Standalone plagiarism checking tool
- Can check individual documents or batches
- Independent of streaming pipeline
- Generates ES-ready format reports

#### `scripts/6_elasticsearch_indexer.py`
- Elasticsearch indexing script
- Reads plagiarism results from HDFS
- Creates Elasticsearch indices
- Bulk indexes results for fast search

#### `scripts/7_dashboard.py`
- Web dashboard Flask application
- RESTful API endpoints for data access
- Connects to Elasticsearch for real-time data
- Serves interactive HTML dashboard
- Provides statistics, charts, and search functionality

#### `scripts/8_full_streamprocess.py`
- Complete streaming pipeline (Steps 2 + 3 + 5 combined)
- Detects new files in stream_input
- Processes files (TF-IDF, similarity)
- Runs batch analysis automatically
- Indexes to Elasticsearch automatically
- Recommended for production use

### Utility Modules

#### `scripts/similarity.py`
- Cosine similarity calculation functions
- SparseVector and DenseVector support
- `compute_similarity_matrix`: calculates similarity between two DataFrames
- `find_plagiarism_candidates`: finds plagiarism cases above threshold

#### `scripts/test_config.py`
- Configuration testing utility
- Verifies HDFS and Spark connections

### Migration & Utility Scripts

#### `migrate_fast.ps1`
- Ultra-fast file migration to HDFS
- Uses Docker volume mounts or tar archives
- Batch processing (500 files per batch)
- Progress tracking and verification

#### `migrate_via_docker.ps1`
- Alternative migration method via Docker
- Uses Docker exec for file transfer

#### `run_migration.bat`
- Batch file wrapper for migration scripts

### Backup & Restore Scripts

#### `backup_docker.ps1`
- Backs up Docker containers and images
- Exports volumes to tar.gz files
- Creates timestamped backups

#### `backup_hdfs_data.ps1`
- Specifically backs up HDFS data
- Creates tar.gz archives of HDFS content

#### `restore_docker.ps1`
- Restores Docker containers and volumes
- Loads images and restores data
- Automates complete restoration process

### Docker Scripts

#### `run_spark_docker.ps1`
- Runs Spark scripts inside Docker container
- Handles environment setup and dependencies

### Dashboard Files

#### `templates/dashboard.html`
- Web dashboard frontend
- Interactive charts using Chart.js
- Real-time statistics display
- Search and filter functionality
- Responsive design

### Documentation

#### `SETUP_TRACKING.md`
- Comprehensive setup and migration tracking
- Step-by-step workflow documentation
- Status of each component
- Troubleshooting guide

#### `DOCKER_SPARK_README.md`
- Documentation for running Spark in Docker
- Configuration and usage instructions

### Other Files

#### `scripts/fix_java_security.ps1`
- PowerShell script to fix Java security file issues
- Creates java.security file with admin privileges

#### `scripts/3_simulateur.py`
- Simulator script (purpose may vary)

---

## Data Flow

### INPUT:
```
Local corpus files (data/corpus_initial/*.txt)
         |
         v
  [Step 0b] Migration to HDFS
         |
         v
  HDFS: /netplag/data/corpus_initial/
```

### PROCESSING:
```
  [Step 1] Batch Initialization
         |
         v
  TF-IDF Model + Reference Vectors
         |
         v
  HDFS: /netplag/storage/idf_model/
  HDFS: /netplag/storage/reference_vectors/

  [Step 2] Streaming Processing
         |
         v
  New files in /netplag/data/stream_input/
         |
         v
  Real-time Plagiarism Detection
         |
         v
  Streaming Vectors + Results
         |
         v
  HDFS: /netplag/storage/streaming_vectors/
  HDFS: /netplag/storage/plagiarism_results/

  [Step 3] Batch Analysis
         |
         v
  Comprehensive Reports
         |
         v
  HDFS: /netplag/storage/reports/
```

### OUTPUT:
```
  [Step 5] Elasticsearch Indexing
         |
         v
  Searchable Indices:
  - plagiarism_reports
  - analysis_results
  - documents
```

---

## Technology Stack

- **Apache Spark 3.5.7**: Distributed processing
- **PySpark**: Python API for Spark
- **HDFS (Hadoop 3.2.1)**: Distributed file storage
- **Elasticsearch 8.11.0**: Search and analytics
- **Flask 2.3+**: Web framework for dashboard
- **Chart.js**: Interactive charts and visualizations
- **Docker**: Containerization (HDFS, Elasticsearch)
- **Python 3.11**: Main programming language
- **Java 17**: Required for Spark

---

## Quick Start

1. **Start services:**
   ```powershell
   docker-compose up -d
   ```

2. **Create HDFS directories:**
   ```powershell
   python scripts/0_migrate_to_hdfs.py
   ```

3. **Migrate data:**
   ```powershell
   .\migrate_fast.ps1
   ```

4. **Initialize batch processing:**
   ```powershell
   python scripts/1_batch_init.py
   ```

5. **Run streaming (optional):**
   ```powershell
   python scripts/2_streaming_app.py
   ```

6. **Analyze results:**
   ```powershell
   python scripts/4_plagiarism_analysis.py
   ```

7. **Index to Elasticsearch:**
   ```powershell
   python scripts/6_elasticsearch_indexer.py
   ```

8. **Start Dashboard:**
   ```powershell
   # Option 1: Docker (Recommended)
   docker-compose up -d dashboard
   
   # Option 2: Local
   python scripts/7_dashboard.py
   
   # Open browser: http://localhost:5000
   ```

9. **Run Full Stream Process (Recommended for Production):**
   ```powershell
   # This combines Steps 2, 3, and 5 automatically
   python scripts/8_full_streamprocess.py
   
   # Add files to HDFS stream_input and they will be:
   # - Processed automatically
   # - Analyzed automatically
   # - Indexed to Elasticsearch automatically
   ```

---

## Additional Resources

- **Setup Guide**: See `SETUP_TRACKING.md` for detailed setup instructions
- **Docker Spark Guide**: See `DOCKER_SPARK_README.md` for Docker-based Spark execution

---

*End of Summary*

