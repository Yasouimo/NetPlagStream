"""
Module de calcul de similarité pour détection de plagiat
Implémente la similarité cosinus pour vecteurs SparseVector PySpark
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.ml.linalg import SparseVector, DenseVector
import numpy as np


def cosine_similarity_sparse(v1, v2):
    """
    Calcule la similarité cosinus entre deux vecteurs SparseVector ou DenseVector
    
    Args:
        v1: SparseVector ou DenseVector PySpark
        v2: SparseVector ou DenseVector PySpark
    
    Returns:
        float: Score de similarité cosinus entre 0 et 1
    """
    if v1 is None or v2 is None:
        return 0.0
    
    # Convertir en numpy arrays pour calcul efficace
    if isinstance(v1, SparseVector):
        v1_array = v1.toArray()
    elif isinstance(v1, DenseVector):
        v1_array = np.array(v1.values)
    else:
        v1_array = np.array(v1)
    
    if isinstance(v2, SparseVector):
        v2_array = v2.toArray()
    elif isinstance(v2, DenseVector):
        v2_array = np.array(v2.values)
    else:
        v2_array = np.array(v2)
    
    # Calculer le produit scalaire
    dot_product = np.dot(v1_array, v2_array)
    
    # Calculer les normes
    norm1 = np.linalg.norm(v1_array)
    norm2 = np.linalg.norm(v2_array)
    
    # Éviter division par zéro
    if norm1 == 0.0 or norm2 == 0.0:
        return 0.0
    
    # Similarité cosinus
    similarity = dot_product / (norm1 * norm2)
    
    # S'assurer que le résultat est entre 0 et 1
    return float(max(0.0, min(1.0, similarity)))


def cosine_similarity_udf():
    """
    Crée une UDF PySpark pour calculer la similarité cosinus
    
    Returns:
        UDF: Fonction UDF pour utilisation dans Spark SQL
    """
    return udf(cosine_similarity_sparse, DoubleType())


def compute_similarity_matrix(df1, df2, vec_col1="features", vec_col2="features", 
                              id_col1="filename", id_col2="filename", top_k=10):
    """
    Calcule la matrice de similarité entre deux DataFrames de vecteurs
    
    Args:
        df1: DataFrame avec les vecteurs à comparer
        df2: DataFrame avec les vecteurs de référence
        vec_col1: Nom de la colonne de vecteurs dans df1
        vec_col2: Nom de la colonne de vecteurs dans df2
        id_col1: Nom de la colonne d'ID dans df1
        id_col2: Nom de la colonne d'ID dans df2
        top_k: Nombre de résultats similaires à retourner
    
    Returns:
        DataFrame: DataFrame avec colonnes [id_col1, id_col2, similarity_score]
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, broadcast
    
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise ValueError("Aucune session Spark active")
    
    # Préparer les DataFrames avec alias
    df1_alias = df1.select(
        col(id_col1).alias("doc_id"),
        col(vec_col1).alias("doc_vector")
    )
    
    df2_alias = df2.select(
        col(id_col2).alias("ref_id"),
        col(vec_col2).alias("ref_vector")
    )
    
    # Cross join pour comparer chaque document avec chaque référence
    # Note: Pour de grandes bases, considérer utiliser broadcast pour df2 si plus petit
    cross_df = df1_alias.crossJoin(
        broadcast(df2_alias) if df2_alias.count() < 10000 else df2_alias
    )
    
    # Calculer la similarité
    similarity_udf = cosine_similarity_udf()
    result_df = cross_df.withColumn(
        "similarity_score",
        similarity_udf(col("doc_vector"), col("ref_vector"))
    )
    
    # Rename columns to avoid conflicts when id_col1 == id_col2
    # Use distinct names: doc_id and ref_id, then alias appropriately
    if id_col1 == id_col2:
        # If both use the same column name, use distinct aliases
        result_df = result_df.select(
            col("doc_id").alias("document_id"),
            col("ref_id").alias("reference_id"),
            col("similarity_score")
        )
    else:
        result_df = result_df.select(
            col("doc_id").alias(id_col1),
            col("ref_id").alias(id_col2),
            col("similarity_score")
        )
    
    # Pour chaque document, garder seulement les top_k plus similaires
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    # Use the appropriate partition column based on whether we renamed
    partition_col = "document_id" if id_col1 == id_col2 else id_col1
    window = Window.partitionBy(partition_col).orderBy(col("similarity_score").desc())
    top_similar = result_df.withColumn(
        "rank",
        row_number().over(window)
    ).filter(col("rank") <= top_k).drop("rank")
    
    return top_similar


def find_plagiarism_candidates(df_docs, df_refs, threshold=0.7, top_k=10,
                                vec_col="features", id_col="filename"):
    """
    Trouve les candidats de plagiat en comparant des documents avec une base de référence
    
    Args:
        df_docs: DataFrame avec les documents à analyser
        df_refs: DataFrame avec les documents de référence
        threshold: Seuil de similarité pour considérer comme plagiat (défaut: 0.7)
        top_k: Nombre maximum de résultats par document (défaut: 10)
        vec_col: Nom de la colonne de vecteurs
        id_col: Nom de la colonne d'ID
    
    Returns:
        DataFrame: DataFrame avec colonnes [document_filename, reference_filename, similarity_score, is_plagiarism]
    """
    from pyspark.sql.functions import when, col
    
    # Calculer les similarités en utilisant les noms de colonnes réels des DataFrames
    # compute_similarity_matrix will handle the case where id_col1 == id_col2
    similarity_df = compute_similarity_matrix(
        df_docs, df_refs, 
        vec_col1=vec_col, vec_col2=vec_col,
        id_col1=id_col, id_col2=id_col,  # Use the actual id_col parameter
        top_k=top_k
    )
    
    # Rename columns to have distinct names for clarity
    # compute_similarity_matrix returns [document_id, reference_id, similarity_score] when id_col1 == id_col2
    # or [id_col1, id_col2, similarity_score] when they differ
    cols = similarity_df.columns
    if "document_id" in cols and "reference_id" in cols:
        # Already has distinct names from compute_similarity_matrix
        similarity_df = similarity_df.withColumnRenamed("document_id", "document_filename") \
                                      .withColumnRenamed("reference_id", "reference_filename")
    elif len(cols) >= 2:
        # Has id_col names, rename them
        similarity_df = similarity_df.withColumnRenamed(cols[0], "document_filename") \
                                      .withColumnRenamed(cols[1], "reference_filename")
    
    # Ajouter colonne is_plagiarism
    result_df = similarity_df.withColumn(
        "is_plagiarism",
        when(col("similarity_score") >= threshold, True).otherwise(False)
    )
    
    return result_df

