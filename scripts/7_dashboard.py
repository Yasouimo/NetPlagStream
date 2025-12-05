"""
NetPlag Dashboard - Web interface for visualizing plagiarism detection results
Provides interactive charts, statistics, and search capabilities
"""

import os
import sys
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request
import json
import time

# Detect if running in Docker
IN_DOCKER = os.path.exists('/.dockerenv') or os.getenv('IN_DOCKER', 'false').lower() == 'true'

# Get project root directory (parent of scripts directory)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
TEMPLATE_DIR = os.path.join(PROJECT_ROOT, 'templates')

# Import Elasticsearch configuration
sys.path.append(PROJECT_ROOT)
from config.elasticsearch_config import ES_URL, ES_INDEX_PLAGIARISM, ES_INDEX_ANALYSIS

try:
    from elasticsearch import Elasticsearch
except ImportError:
    print("[ERROR] Elasticsearch client not installed. Run: pip install 'elasticsearch>=8.11.0,<9.0.0'")
    sys.exit(1)

# Initialize Flask app with correct template folder
app = Flask(__name__, template_folder=TEMPLATE_DIR)

# Initialize Elasticsearch client with retry logic
es = None

def connect_elasticsearch(retries=5, delay=5):
    """Connect to Elasticsearch with retry logic"""
    global es
    for i in range(retries):
        try:
            es = Elasticsearch([ES_URL], request_timeout=30)
            if es.ping():
                print(f"[OK] Connected to Elasticsearch at {ES_URL}")
                return True
        except Exception as e:
            if i < retries - 1:
                print(f"[WARN] Connection attempt {i+1}/{retries} failed: {e}")
                print(f"[INFO] Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"[ERROR] Cannot connect to Elasticsearch after {retries} attempts: {e}")
                print(f"[ERROR] URL: {ES_URL}")
                print("[ERROR] Make sure Elasticsearch is running: docker-compose up -d elasticsearch")
    return False

# Try to connect on startup
if not connect_elasticsearch():
    print("[WARN] Starting dashboard without Elasticsearch connection")
    print("[INFO] Dashboard will show connection errors. Elasticsearch may still be starting...")

def ensure_elasticsearch():
    """Ensure Elasticsearch connection is active, reconnect if needed"""
    global es
    if es is None:
        if not connect_elasticsearch(retries=2, delay=2):
            return False
    else:
        try:
            if not es.ping():
                print("[WARN] Elasticsearch connection lost, reconnecting...")
                if not connect_elasticsearch(retries=2, delay=2):
                    return False
        except:
            print("[WARN] Elasticsearch ping failed, reconnecting...")
            es = None
            if not connect_elasticsearch(retries=2, delay=2):
                return False
    return True

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/stats')
def get_stats():
    """Get overall statistics"""
    if not ensure_elasticsearch():
        return jsonify({"error": "Elasticsearch not connected. Please check if Elasticsearch is running."}), 503
    
    try:
        # Get plagiarism reports count
        plagiarism_count = es.count(index=ES_INDEX_PLAGIARISM)['count']
        
        # Get analysis count
        analysis_count = es.count(index=ES_INDEX_ANALYSIS)['count']
        
        # Get high similarity cases (>0.8)
        high_similarity_query = {
            "query": {
                "range": {
                    "similarity_score": {
                        "gt": 0.8
                    }
                }
            }
        }
        high_similarity_count = es.count(index=ES_INDEX_PLAGIARISM, body=high_similarity_query)['count']
        
        # Get confirmed plagiarism cases
        plagiarism_query = {
            "query": {
                "term": {
                    "is_plagiarism": True
                }
            }
        }
        confirmed_plagiarism = es.count(index=ES_INDEX_PLAGIARISM, body=plagiarism_query)['count']
        
        # Get average similarity score
        avg_score_query = {
            "aggs": {
                "avg_similarity": {
                    "avg": {
                        "field": "similarity_score"
                    }
                },
                "max_similarity": {
                    "max": {
                        "field": "similarity_score"
                    }
                },
                "min_similarity": {
                    "min": {
                        "field": "similarity_score"
                    }
                }
            },
            "size": 0
        }
        stats_result = es.search(index=ES_INDEX_PLAGIARISM, body=avg_score_query)
        avg_score = stats_result['aggregations']['avg_similarity']['value'] or 0
        max_score = stats_result['aggregations']['max_similarity']['value'] or 0
        min_score = stats_result['aggregations']['min_similarity']['value'] or 0
        
        return jsonify({
            "total_cases": plagiarism_count,
            "analysis_count": analysis_count,
            "high_similarity": high_similarity_count,
            "confirmed_plagiarism": confirmed_plagiarism,
            "avg_similarity": round(avg_score, 4),
            "max_similarity": round(max_score, 4),
            "min_similarity": round(min_score, 4)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/plagiarism_cases')
def get_plagiarism_cases():
    """Get plagiarism cases with pagination"""
    if not ensure_elasticsearch():
        return jsonify({"error": "Elasticsearch not connected. Please check if Elasticsearch is running."}), 503
    
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 20))
        min_score = float(request.args.get('min_score', 0.0))
        only_plagiarism = request.args.get('only_plagiarism', 'false').lower() == 'true'
        
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "similarity_score": {
                                    "gte": min_score
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "similarity_score": {
                        "order": "desc"
                    }
                }
            ],
            "from": (page - 1) * per_page,
            "size": per_page
        }
        
        if only_plagiarism:
            query["query"]["bool"]["must"].append({
                "term": {
                    "is_plagiarism": True
                }
            })
        
        result = es.search(index=ES_INDEX_PLAGIARISM, body=query)
        
        cases = []
        for hit in result['hits']['hits']:
            source = hit['_source']
            cases.append({
                "document": source.get('document_filename', ''),
                "reference": source.get('reference_filename', ''),
                "similarity": round(source.get('similarity_score', 0), 4),
                "is_plagiarism": source.get('is_plagiarism', False),
                "timestamp": source.get('timestamp', '')
            })
        
        return jsonify({
            "cases": cases,
            "total": result['hits']['total']['value'],
            "page": page,
            "per_page": per_page
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/similarity_distribution')
def get_similarity_distribution():
    """Get similarity score distribution for histogram"""
    if not ensure_elasticsearch():
        return jsonify({"error": "Elasticsearch not connected. Please check if Elasticsearch is running."}), 503
    
    try:
        query = {
            "aggs": {
                "similarity_ranges": {
                    "histogram": {
                        "field": "similarity_score",
                        "interval": 0.1,
                        "min_doc_count": 0
                    }
                }
            },
            "size": 0
        }
        
        result = es.search(index=ES_INDEX_PLAGIARISM, body=query)
        
        buckets = result['aggregations']['similarity_ranges']['buckets']
        
        labels = [f"{bucket['key']:.1f}-{bucket['key']+0.1:.1f}" for bucket in buckets]
        values = [bucket['doc_count'] for bucket in buckets]
        
        return jsonify({
            "labels": labels,
            "values": values
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/top_documents')
def get_top_documents():
    """Get top documents with most plagiarism matches"""
    if not ensure_elasticsearch():
        return jsonify({"error": "Elasticsearch not connected. Please check if Elasticsearch is running."}), 503
    
    try:
        query = {
            "aggs": {
                "top_documents": {
                    "terms": {
                        "field": "document_filename",
                        "size": 10,
                        "order": {
                            "_count": "desc"
                        }
                    },
                    "aggs": {
                        "avg_similarity": {
                            "avg": {
                                "field": "similarity_score"
                            }
                        },
                        "max_similarity": {
                            "max": {
                                "field": "similarity_score"
                            }
                        }
                    }
                }
            },
            "size": 0
        }
        
        result = es.search(index=ES_INDEX_PLAGIARISM, body=query)
        
        documents = []
        for bucket in result['aggregations']['top_documents']['buckets']:
            documents.append({
                "document": bucket['key'],
                "match_count": bucket['doc_count'],
                "avg_similarity": round(bucket['avg_similarity']['value'] or 0, 4),
                "max_similarity": round(bucket['max_similarity']['value'] or 0, 4)
            })
        
        return jsonify({"documents": documents})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/analysis_summary')
def get_analysis_summary():
    """Get analysis summary data"""
    if not ensure_elasticsearch():
        return jsonify({"error": "Elasticsearch not connected. Please check if Elasticsearch is running."}), 503
    
    try:
        query = {
            "query": {
                "match_all": {}
            },
            "sort": [
                {
                    "max_score": {
                        "order": "desc"
                    }
                }
            ],
            "size": 20
        }
        
        result = es.search(index=ES_INDEX_ANALYSIS, body=query)
        
        summaries = []
        for hit in result['hits']['hits']:
            source = hit['_source']
            summaries.append({
                "document": source.get('document_filename', ''),
                "num_matches": source.get('num_matches', 0),
                "max_score": round(source.get('max_score', 0), 4),
                "avg_score": round(source.get('avg_score', 0), 4),
                "timestamp": source.get('analysis_timestamp', '')
            })
        
        return jsonify({"summaries": summaries})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/search')
def search():
    """Search plagiarism cases"""
    if not ensure_elasticsearch():
        return jsonify({"error": "Elasticsearch not connected. Please check if Elasticsearch is running."}), 503
    
    try:
        search_term = request.args.get('q', '')
        min_score = float(request.args.get('min_score', 0.0))
        
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "similarity_score": {
                                    "gte": min_score
                                }
                            }
                        }
                    ],
                    "should": [
                        {
                            "wildcard": {
                                "document_filename": f"*{search_term}*"
                            }
                        },
                        {
                            "wildcard": {
                                "reference_filename": f"*{search_term}*"
                            }
                        }
                    ],
                    "minimum_should_match": 1 if search_term else 0
                }
            },
            "sort": [
                {
                    "similarity_score": {
                        "order": "desc"
                    }
                }
            ],
            "size": 50
        }
        
        result = es.search(index=ES_INDEX_PLAGIARISM, body=query)
        
        cases = []
        for hit in result['hits']['hits']:
            source = hit['_source']
            cases.append({
                "document": source.get('document_filename', ''),
                "reference": source.get('reference_filename', ''),
                "similarity": round(source.get('similarity_score', 0), 4),
                "is_plagiarism": source.get('is_plagiarism', False)
            })
        
        return jsonify({
            "cases": cases,
            "total": result['hits']['total']['value']
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/health')
def health_check():
    """Check Elasticsearch connection status"""
    if ensure_elasticsearch():
        try:
            info = es.info()
            return jsonify({
                "status": "connected",
                "elasticsearch_version": info['version']['number'],
                "cluster_name": info['cluster_name'],
                "url": ES_URL
            })
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": f"Cannot get Elasticsearch info: {str(e)}"
            }), 500
    else:
        return jsonify({
            "status": "disconnected",
            "url": ES_URL,
            "message": "Elasticsearch is not connected. Please check if Elasticsearch is running."
        }), 503

if __name__ == '__main__':
    print("\n" + "="*60)
    print("  NETPLAG DASHBOARD")
    print("="*60)
    print(f"[INFO] Starting dashboard server...")
    print(f"[INFO] Environment: {'Docker' if IN_DOCKER else 'Windows Host'}")
    print(f"[INFO] Elasticsearch URL: {ES_URL}")
    if IN_DOCKER:
        print(f"[INFO] Dashboard will be available at: http://localhost:5000 (from host)")
        print(f"[INFO] Dashboard internal URL: http://dashboard:5000 (from Docker network)")
    else:
        print(f"[INFO] Dashboard will be available at: http://localhost:5000")
    print("="*60 + "\n")
    
    # In Docker, use debug=False for production
    # In development (Windows host), use debug=True
    debug_mode = not IN_DOCKER
    app.run(host='0.0.0.0', port=5000, debug=debug_mode)

