"""
Script d'analyse standalone complète de plagiat
Interface pour analyser un document ou un batch
Génère des rapports détaillés avec format ES-ready
"""

import os
import sys
import json
from datetime import datetime

os.environ['JAVA_HOME'] = r'D:\jdk17'
os.environ['HADOOP_HOME'] = r'D:\hadoopbin\winutils\hadoop-3.0.1'
os.environ['SPARK_HOME'] = r'D:\DOWNLOAD\spark-3.5.7-bin-hadoop3\spark-3.5.7-bin-hadoop3'
# Ajouter Java et Hadoop au PATH
java_bin = os.path.join(os.environ['JAVA_HOME'], 'bin')
hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
current_path = os.environ.get('PATH', '')
os.environ['PATH'] = f"{java_bin};{hadoop_bin};{current_path}"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, Row
from pyspark.ml.feature import HashingTF, IDF, IDFModel
from pyspark.sql.functions import col, udf, input_file_name, max as spark_max, avg, desc, count
from pyspark.sql.types import ArrayType, StringType
import re

from config.hdfs_config import (
    HDFS_REFERENCE_VECTORS, HDFS_IDF_MODEL, HDFS_REPORTS,
    get_spark_hdfs_config
)
from scripts.similarity import find_plagiarism_candidates

# Configuration par défaut
DEFAULT_THRESHOLD = 0.7
DEFAULT_TOP_K = 10
DEFAULT_NUM_FEATURES = 5000


def clean_text(text):
    """Fonction de nettoyage de texte"""
    if not text: 
        return []
    text = text.lower()[:50000]
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    words = text.split()
    words = [w for w in words if len(w) > 2]
    return words


def vectorize_documents(spark, documents, idf_model=None):
    """
    Vectorise une liste de documents
    
    Args:
        spark: SparkSession
        documents: Liste de tuples (filename, content)
        idf_model: Modèle IDF optionnel
    
    Returns:
        DataFrame avec colonnes [filename, features]
    """
    # Créer DataFrame
    df = spark.createDataFrame(documents, ["filename", "content"])
    
    # Nettoyer le texte
    clean_udf = udf(clean_text, ArrayType(StringType()))
    df_clean = df.withColumn("words", clean_udf(col("content")))
    
    # Appliquer TF
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=DEFAULT_NUM_FEATURES)
    df_tf = hashingTF.transform(df_clean)
    
    # Appliquer IDF si modèle disponible
    if idf_model is not None:
        df_vectors = idf_model.transform(df_tf).select("filename", col("features").alias("features"))
    else:
        df_vectors = df_tf.select("filename", col("rawFeatures").alias("features"))
    
    return df_vectors


def analyze_document(spark, document_path, threshold=DEFAULT_THRESHOLD, top_k=DEFAULT_TOP_K):
    """
    Analyse un document unique pour détecter le plagiat
    
    Args:
        spark: SparkSession
        document_path: Chemin vers le document (local ou HDFS)
        threshold: Seuil de similarité
        top_k: Nombre de résultats à retourner
    
    Returns:
        dict: Résultats de l'analyse
    """
    print(f"\nAnalyse du document: {document_path}")
    
    # Lire le document
    try:
        if document_path.startswith("hdfs://"):
            content = spark.read.text(document_path, wholetext=True).collect()[0]["value"]
            filename = document_path.split("/")[-1]
        else:
            with open(document_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(50000)
            filename = os.path.basename(document_path)
    except Exception as e:
        print(f"❌ Erreur lecture document: {e}")
        return None
    
    # Charger modèle IDF
    try:
        idf_model = IDFModel.load(HDFS_IDF_MODEL)
        print("✓ Modèle IDF chargé")
    except:
        print("⚠ Modèle IDF non disponible, utilisation TF uniquement")
        idf_model = None
    
    # Vectoriser le document
    documents = [(filename, content)]
    df_doc = vectorize_documents(spark, documents, idf_model)
    
    # Charger vecteurs de référence
    df_reference = spark.read.parquet(HDFS_REFERENCE_VECTORS)
    print(f"✓ Corpus de référence chargé: {df_reference.count()} documents")
    
    # Calculer similarité
    results = find_plagiarism_candidates(
        df_doc, df_reference,
        threshold=threshold,
        top_k=top_k,
        vec_col="features",
        id_col="filename"
    )
    
    # Collecter résultats
    # Note: results has columns: document_filename, reference_filename, similarity_score, is_plagiarism
    plagiarism_cases = results.filter(col("is_plagiarism") == True) \
        .orderBy(col("similarity_score").desc()) \
        .collect()
    
    # Préparer résultats
    analysis_result = {
        "document_id": filename,
        "timestamp": datetime.now().isoformat(),
        "threshold": threshold,
        "plagiarism_detected": len(plagiarism_cases) > 0,
        "max_similarity": plagiarism_cases[0]["similarity_score"] if plagiarism_cases else 0.0,
        "top_matches": [
            {
                "reference_id": row["reference_filename"],
                "similarity_score": float(row["similarity_score"]),
                "is_plagiarism": row["is_plagiarism"]
            }
            for row in plagiarism_cases[:top_k]
        ],
        "total_matches": len(plagiarism_cases)
    }
    
    return analysis_result


def analyze_batch(spark, document_paths, threshold=DEFAULT_THRESHOLD, top_k=DEFAULT_TOP_K):
    """
    Analyse un batch de documents
    
    Args:
        spark: SparkSession
        document_paths: Liste de chemins vers les documents
        threshold: Seuil de similarité
        top_k: Nombre de résultats par document
    
    Returns:
        list: Liste de résultats d'analyse
    """
    print(f"\nAnalyse de {len(document_paths)} documents...")
    
    # Charger modèle IDF
    try:
        idf_model = IDFModel.load(HDFS_IDF_MODEL)
        print("✓ Modèle IDF chargé")
    except:
        print("⚠ Modèle IDF non disponible, utilisation TF uniquement")
        idf_model = None
    
    # Lire et vectoriser tous les documents
    documents = []
    for doc_path in document_paths:
        try:
            if doc_path.startswith("hdfs://"):
                content = spark.read.text(doc_path, wholetext=True).collect()[0]["value"]
                filename = doc_path.split("/")[-1]
            else:
                with open(doc_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read(50000)
                filename = os.path.basename(doc_path)
            documents.append((filename, content))
        except Exception as e:
            print(f"⚠ Erreur lecture {doc_path}: {e}")
    
    if not documents:
        print("❌ Aucun document valide à analyser")
        return []
    
    df_docs = vectorize_documents(spark, documents, idf_model)
    print(f"✓ {len(documents)} documents vectorisés")
    
    # Charger vecteurs de référence
    df_reference = spark.read.parquet(HDFS_REFERENCE_VECTORS)
    print(f"✓ Corpus de référence chargé: {df_reference.count()} documents")
    
    # Calculer similarité
    print("Calcul de similarité...")
    results = find_plagiarism_candidates(
        df_docs, df_reference,
        threshold=threshold,
        top_k=top_k,
        vec_col="features",
        id_col="filename"
    )
    
    # Grouper par document
    # Note: results has columns: document_filename, reference_filename, similarity_score, is_plagiarism
    results_by_doc = {}
    for row in results.collect():
        doc_id = row["document_filename"]
        if doc_id not in results_by_doc:
            results_by_doc[doc_id] = []
        results_by_doc[doc_id].append({
            "reference_id": row["reference_filename"],
            "similarity_score": float(row["similarity_score"]),
            "is_plagiarism": row["is_plagiarism"]
        })
    
    # Créer résultats format ES-ready
    analysis_results = []
    for doc_id, matches in results_by_doc.items():
        plagiarism_matches = [m for m in matches if m["is_plagiarism"]]
        max_score = max([m["similarity_score"] for m in matches]) if matches else 0.0
        
        analysis_results.append({
            "document_id": doc_id,
            "timestamp": datetime.now().isoformat(),
            "threshold": threshold,
            "plagiarism_detected": len(plagiarism_matches) > 0,
            "max_similarity": max_score,
            "top_matches": sorted(matches, key=lambda x: x["similarity_score"], reverse=True)[:top_k],
            "total_matches": len(plagiarism_matches)
        })
    
    return analysis_results


def save_report_es_ready(results, output_path):
    """
    Sauvegarde les résultats dans un format compatible Elasticsearch
    
    Args:
        results: Liste de résultats d'analyse
        output_path: Chemin de sortie (HDFS ou local)
    """
    # Format ES-ready: un document JSON par ligne
    es_documents = []
    for result in results:
        es_doc = {
            "index": {
                "_index": "plagiarism_reports",
                "_type": "_doc"
            }
        }
        es_documents.append(json.dumps(es_doc))
        es_documents.append(json.dumps(result))
    
    # Sauvegarder
    if output_path.startswith("hdfs://"):
        # Pour HDFS, utiliser Spark pour écrire
        spark = SparkSession.getActiveSession()
        if spark:
            rdd = spark.sparkContext.parallelize(es_documents)
            rdd.coalesce(1).saveAsTextFile(output_path)
    else:
        # Local file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(es_documents))
    
    print(f"✓ Rapport sauvegardé (format ES-ready): {output_path}")


def main():
    """Fonction principale"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyse standalone de plagiat')
    parser.add_argument('--document', type=str, help='Chemin vers un document à analyser')
    parser.add_argument('--batch', type=str, nargs='+', help='Chemins vers plusieurs documents')
    parser.add_argument('--threshold', type=float, default=DEFAULT_THRESHOLD, help=f'Seuil de similarité (défaut: {DEFAULT_THRESHOLD})')
    parser.add_argument('--top-k', type=int, default=DEFAULT_TOP_K, help=f'Nombre de résultats (défaut: {DEFAULT_TOP_K})')
    parser.add_argument('--output', type=str, help='Chemin de sortie pour le rapport')
    
    args = parser.parse_args()
    
    # Initialisation Spark
    spark_config = {
        "spark.app.name": "NetPlag_Standalone_Check",
        "spark.master": "local[4]",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g"
    }
    spark_config.update(get_spark_hdfs_config())
    
    spark = SparkSession.builder
    for key, value in spark_config.items():
        spark = spark.config(key, value)
    spark = spark.getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== ANALYSE STANDALONE DE PLAGIAT ===")
    
    try:
        if args.document:
            # Analyse d'un document unique
            result = analyze_document(spark, args.document, args.threshold, args.top_k)
            if result:
                print("\n=== RÉSULTATS ===")
                print(json.dumps(result, indent=2, ensure_ascii=False))
                
                if args.output:
                    save_report_es_ready([result], args.output)
                else:
                    # Sauvegarder par défaut sur HDFS
                    default_output = f"{HDFS_REPORTS}/standalone_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    save_report_es_ready([result], default_output)
        
        elif args.batch:
            # Analyse batch
            results = analyze_batch(spark, args.batch, args.threshold, args.top_k)
            if results:
                print(f"\n=== RÉSULTATS ({len(results)} documents) ===")
                for result in results:
                    print(f"\nDocument: {result['document_id']}")
                    print(f"  Plagiat détecté: {result['plagiarism_detected']}")
                    print(f"  Similarité max: {result['max_similarity']:.4f}")
                    print(f"  Nombre de matches: {result['total_matches']}")
                
                if args.output:
                    save_report_es_ready(results, args.output)
                else:
                    # Sauvegarder par défaut sur HDFS
                    default_output = f"{HDFS_REPORTS}/standalone_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    save_report_es_ready(results, default_output)
        
        else:
            print("❌ Spécifiez --document ou --batch")
            parser.print_help()
    
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

