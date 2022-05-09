from connection import Neo4jConnection
import pandas as pd
import config

conn = Neo4jConnection(uri=config.uri, user=config.user, pwd=config.pwd)


def jaccard_similarity(prop_link1, prop_link2):
    query = f"""
    RETURN gds.similarity.jaccard({prop_link1}, {prop_link2}) AS jaccardSimilarity
    """
    response = [dict(_) for _ in conn.query(query, db=config.db)]
    return response


def overlap_similarity(prop_link1, prop_link2):
    query = f"""
    RETURN gds.similarity.overlap({prop_link1}, {prop_link2}) AS overlapSimilarity
    """
    response = [dict(_) for _ in conn.query(query, db=config.db)]
    return response


def cosine_similarity(prop_link1, prop_link2):
    query = f"""
    RETURN gds.similarity.cosine({prop_link1}, {prop_link2}) AS cosineSimilarity
    """
    response = [dict(_) for _ in conn.query(query, db=config.db)]
    return response


def pearson_similarity(prop_link1, prop_link2):
    query = f"""
    RETURN gds.similarity.pearson({prop_link1}, {prop_link2}) AS pearsonSimilarity
    """
    response = [dict(_) for _ in conn.query(query, db=config.db)]
    return response


def euclidean_similarity(prop_link1, prop_link2):
    query = f"""
    RETURN gds.similarity.euclidean({prop_link1}, {prop_link2}) AS euclideanSimilarity
    """
    response = [dict(_) for _ in conn.query(query, db=config.db)]
    return response


def euclideanDistance_similarity(prop_link1, prop_link2):
    query = f"""
    RETURN gds.similarity.euclideanDistance({prop_link1}, {prop_link2}) AS euclideanDistance
    """
    response = [dict(_) for _ in conn.query(query, db=config.db)]
    return response


if __name__ == "__main__":
    print(jaccard_similarity([1.0, 5.0, 3.0, 6.7], [5.0, 2.5, 3.1, 9.0]))
    print(overlap_similarity([1.0, 5.0, 3.0, 6.7], [5.0, 2.5, 3.1, 9.0]))
    print(cosine_similarity([1.0, 5.0, 3.0, 6.7], [5.0, 2.5, 3.1, 9.0]))
    print(pearson_similarity([1.0, 5.0, 3.0, 6.7], [5.0, 2.5, 3.1, 9.0]))
    print(euclidean_similarity([1.0, 5.0, 3.0, 6.7], [5.0, 2.5, 3.1, 9.0]))
    print(euclideanDistance_similarity([1.0, 5.0, 3.0, 6.7], [5.0, 2.5, 3.1, 9.0]))