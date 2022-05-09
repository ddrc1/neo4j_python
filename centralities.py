from connection import Neo4jConnection
import pandas as pd
import config

conn = Neo4jConnection(uri=config.uri, user=config.user, pwd=config.pwd)


def compile_gds_graph(mode="UNDIRECTED", name="social_graph"):
    query = """
    CALL gds.graph.project(
        '%s', 'User', {ACTION: {orientation: '%s'}},
        {
            relationshipProperties: ['weight', 'FEATURE0', 'FEATURE1', 'FEATURE2', 'FEATURE3', 'LABEL']
        }
    )""" % (name, mode)
    conn.query(query, db=config.db)


def shortestPathDijkstra(origin_node, destination_node, name, weightProperty="weight"):
    query = """
        MATCH (source:User {userid: %i}), (destination:User {userid: %i})
        CALL gds.shortestPath.dijkstra.stream('%s', {
            sourceNode: source,
            targetNode: destination,
            relationshipWeightProperty: '%s'
        })
        YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs
        RETURN
            index,
            gds.util.asNode(sourceNode).userid AS sourceName,
            gds.util.asNode(targetNode).userid AS targetName,
            totalCost,
            [nodeId IN nodeIds | gds.util.asNode(nodeId).userid] AS pathNames,
            costs
        ORDER BY index
    """ % (origin_node, destination_node, name, weightProperty)
    response = pd.DataFrame([dict(_) for _ in conn.query(query, db=config.db)])
    return response


def allShortestPaths(node_id, name, weightProperty="weight"):
    query = """
        MATCH (source:User {userid: %i})
        CALL gds.allShortestPaths.dijkstra.stream('%s', {
            sourceNode: source,
            relationshipWeightProperty: '%s'
        })
        YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs
    RETURN
        index,
        gds.util.asNode(sourceNode).userid AS sourceName,
        gds.util.asNode(targetNode).userid AS targetName,
        totalCost,
        [nodeId IN nodeIds | gds.util.asNode(nodeId).userid] AS nodeNames, costs
    """ % (node_id, name, weightProperty)
    response = pd.DataFrame([dict(_) for _ in conn.query(query, db=config.db)])
    return response


def BFS(node_id, name): #?????
    query = """
        MATCH (source:User {userid: %i})
        CALL gds.bfs.stream('%s', {
            sourceNode: source
        })
        YIELD path
        RETURN path
    """ % (node_id, name)
    conn.query(query, db=config.db)
    response = conn.query(query, db=config.db)
    return response


def DFS(node_id, name): #?????
    query = """
        MATCH (source:User {userid: %i})
        CALL gds.dfs.stream('%s', {
            sourceNode: source
        })
        YIELD path
        RETURN path
    """ % (node_id, name)
    response = conn.query(query, db=config.db)
    #response = pd.DataFrame([dict(_) for _ in conn.query(query, db=config.db)])
    return response#[dict(_) for _ in conn.query(query, db=config.db)]#response.path


def degree(name):
    query = """
        CALL gds.degree.stream('%s')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).userid AS ID, score AS connections
        ORDER BY connections DESC, ID DESC
    """ % name
    conn.query(query, db=config.db)
    response = pd.DataFrame([dict(_) for _ in conn.query(query, db=config.db)])
    return response


def closeness(name):
    query = """
        CALL gds.beta.closeness.stream('%s')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).userid AS ID, score AS connections
        ORDER BY connections DESC, ID DESC
    """ % name
    conn.query(query, db=config.db)
    response = pd.DataFrame([dict(_) for _ in conn.query(query, db=config.db)])
    return response


def betweenness(name):
    query = """
        CALL gds.betweenness.stream('%s')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).userid AS ID, score AS connections
        ORDER BY connections DESC, ID DESC
    """ % name
    conn.query(query, db=config.db)
    response = pd.DataFrame([dict(_) for _ in conn.query(query, db=config.db)])
    return response


def eigenvector(name, weightProperty="weight"):
    query = """
        CALL gds.eigenvector.stream('%s', {
            relationshipWeightProperty: '%s'
        })
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).userid AS ID, score AS connections
        ORDER BY connections DESC, ID DESC
    """ % (name, weightProperty)
    response = pd.DataFrame([dict(_) for _ in conn.query(query, db=config.db)])
    return response


def pageRank(name, weightProperty="weight"):
    query = """
        CALL gds.pageRank.stream('%s', { 
            dampingFactor: 0.85, 
            relationshipWeightProperty: '%s' })
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).userid AS ID, score
        ORDER BY score DESC, ID ASC
    """ % (name, weightProperty)
    response = pd.DataFrame([dict(_) for _ in conn.query(query, db=config.db)])
    return response

if __name__ == "__main__":
    name = "social_graph_undirected"
    #compile_gds_graph(name=name)
    #print(shortestPathDijkstra(0, 70, name))
    #print(pageRank(name))
    #print(eigenvector(name))
    print(betweenness(name)) 
    print(closeness(name))
    print(degree(name))
    #print(DFS(3168, name))
    #print(BFS(3168, name))
    #print(allShortestPaths(3168, name))
    #print(shortestPathDijkstra(3168, 47, name))