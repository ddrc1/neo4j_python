from connection import Neo4jConnection
import pandas as pd
import config
import time


def delete_node(): #0.007348299026489258s
    print("Deleting nodes...", end="")
    start = time.time()

    query = "MATCH (u) DETACH DELETE u"
    conn.query(query, db=config.db)
    end = time.time()
    print(f" Done in {end-start}s")


def create_node(): #12.240525960922241s
    delete_node()

    print("Creating nodes...")
    start = time.time()

    origin_node = graph.drop_duplicates(subset='USERID')["USERID"].tolist()
    target_node = graph.drop_duplicates(subset='TARGETID')["TARGETID"].tolist()
    nodes = set(origin_node + target_node)

    for user_id in nodes:
        conn.query("CREATE (u: User {userid: toInteger(%i)})" % user_id , db=config.db)

    end = time.time()
    print(f"Done in {end-start}s")


def create_link(): #1327.1478402614594s
    print("Creating links...")
    start = time.time()

    query = """
    WITH "file:///graph.tsv" AS file
        LOAD CSV WITH HEADERS FROM file AS row
            MATCH (target:User {userid: toInteger(row.TARGETID)})
            MATCH (origin:User {userid: toInteger(row.USERID)})
            MERGE (origin)-[:ACTION {weight: 1, FEATURE0: toFloat(row.FEATURE0), FEATURE1: toFloat(row.FEATURE1), FEATURE2: toFloat(row.FEATURE2), FEATURE3: toFloat(row.FEATURE3), LABEL: toInteger(row.LABEL)}]->(target)
    """
    conn.query(query, db=config.db)

    end = time.time()
    print(f"Done in {end-start}s")

if __name__ == "__main__":
    conn = Neo4jConnection(uri=config.uri, user=config.user, pwd=config.pwd)
    print("Reading file...")
    graph = pd.read_csv("./graph.csv")

    create_node()
    create_link()