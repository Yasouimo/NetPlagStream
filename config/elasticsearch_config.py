"""
Configuration Elasticsearch pour NetPlag
Centralise les paramètres de connexion et les noms d'indices
"""

import os

# Detect if running in Docker
IN_DOCKER = os.path.exists('/.dockerenv') or os.getenv('IN_DOCKER', 'false').lower() == 'true'

# Elasticsearch configuration
if IN_DOCKER:
    ES_HOST = os.getenv("ES_HOST", "elasticsearch")
else:
    ES_HOST = os.getenv("ES_HOST", "localhost")

ES_PORT = int(os.getenv("ES_PORT", "9200"))
ES_URL = f"http://{ES_HOST}:{ES_PORT}"

# Index names
ES_INDEX_PLAGIARISM = "plagiarism_reports"
ES_INDEX_DOCUMENTS = "documents"
ES_INDEX_ANALYSIS = "analysis_results"

# Index settings and mappings
PLAGIARISM_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "document_filename": {
                "type": "keyword"
            },
            "reference_filename": {
                "type": "keyword"
            },
            "similarity_score": {
                "type": "float"
            },
            "is_plagiarism": {
                "type": "boolean"
            },
            "timestamp": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            },
            "batch_id": {
                "type": "keyword"
            }
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s"
    }
}

DOCUMENT_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "filename": {
                "type": "keyword"
            },
            "content_preview": {
                "type": "text",
                "analyzer": "standard"
            },
            "word_count": {
                "type": "integer"
            },
            "timestamp": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            },
            "source": {
                "type": "keyword"  # "corpus_initial", "stream_input", etc.
            }
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}

ANALYSIS_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "document_filename": {
                "type": "keyword"
            },
            "num_matches": {
                "type": "integer"
            },
            "max_score": {
                "type": "float"
            },
            "avg_score": {
                "type": "float"
            },
            "analysis_timestamp": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            }
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}

