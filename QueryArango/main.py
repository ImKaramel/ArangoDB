import json
import sys
import requests
# from pyArango.connection import *


path = "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/QueryArango/"

class ArangoDBQuery:
    def __init__(self):
        self.url = 'http://localhost:8529'
        self.db_name = '_system'
        self.username = 'root'
        self.password = '1'
        self.auth = (self.username, self.password)
        self.endpoint = f'{self.url}/_db/{self.db_name}/_api/cursor'

    def getStats(self, graph, nameQuery, explanation):
        time = explanation["stats"]["executionTime"]
        memory = explanation["stats"]["peakMemoryUsage"]

        with open(path + "stats/stats" + graph , "a") as file:
            file.write(nameQuery + "\n")
            file.write("executionTime ")
            file.write(str(time) + " s" + "\n")
            file.write("peakMemoryUsage " + str(memory) + " byte" + "\n\n\n")

    def queryFilter(self, graph, collection, fieldName, value):

        headers = {'Content-Type': 'application/json'}
        data = {'query':
                    f'''FOR v IN {collection} 
                        FILTER v.{fieldName} >= {value} 
                        RETURN {{"id": v._id, "{fieldName}": v.{fieldName}
                        }}''',
                'optimizer': {'rules': ['use-index-range']}
                }

        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)
        if response.status_code == 201:
            result = response.json()['result']
            extra = response.json()['extra']
            self.getStats(graph, "queryFilter", extra)
            with open(f"results/results{graph}/queryFilter.json", "w") as file:
                json.dump(result, file, indent=4)

            return result
        else:
            print(f"Ошибка при выполнении запроса queryFilter: {response.text}")

    def queryBFS(self, graph, depth, startVertex, param, value):

        headers = {'Content-Type': 'application/json'}
        if graph == "Elliptic":
            data = {
                'query': f'''
                            FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
                            OPTIONS {{"order": "bfs"}}
                            FILTER LENGTH(p.edges) == {depth} && (p.vertices[*].{param} ALL >= {value})
                            FILTER LENGTH(p.vertices) == {depth + 1}
                            RETURN  {{'vertex':v._id, '{param}': p.vertices[*].{param}}}
                        '''
            }
        elif graph == "RoadNet":
            data = {
                'query': f'''
                                FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
                                OPTIONS {{"order": "bfs"}}
                                FILTER LENGTH(p.edges) == {depth}
                                FILTER LENGTH(p.vertices) == {depth + 1}
                                RETURN  {{'vertex':v._id}}
                            '''
            }
        else:
            data = {
                'query': f'''
                    FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
                    OPTIONS {{"order": "bfs"}}
                    FILTER LENGTH(p.edges) == {depth} && (p.edges[*].{param} ALL >= {value})
                    FILTER LENGTH(p.vertices) == {depth + 1}
                    RETURN  {{'vertex':v._id, '{param}': p.edges[*].{param}}}
                '''
            }


        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)
        if response.status_code == 201:
            result = response.json()['result']
            extra = response.json()['extra']
            self.getStats(graph, "queryBFS", extra)
            with open(f"results/results{graph}/queryBFS.json", "w") as file:
                json.dump(result, file, indent=4)
            return result
        else:
            print(f"Ошибка при выполнении запроса queryBFS: {response.text}")

    def queryDFS(self, graph, depth, startVertex, param, value):


        headers = {'Content-Type': 'application/json'}

        if graph == "Elliptic":
            data = {
                'query': f'''
                            FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
                            OPTIONS {{"order": "dfs"}}
                            FILTER LENGTH(p.edges) == {depth} && (p.vertices[*].{param} ALL >= {value})
                            FILTER LENGTH(p.vertices) == {depth + 1}
                            RETURN  {{'vertex':v._id, '{param}': p.vertices[*].{param}}}
                        '''
            }
        elif graph == "RoadNet":
            data = {
                'query': f'''
                                       FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
                                       OPTIONS {{"order": "dfs"}}
                                       FILTER LENGTH(p.edges) == {depth}
                                       FILTER LENGTH(p.vertices) == {depth + 1}
                                       RETURN  {{'vertex':v._id}}
                                   '''
            }
        else:
            data = {
                'query': f'''
                    FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
                    OPTIONS {{"order": "dfs"}}
                    FILTER LENGTH(p.edges) == {depth} && (p.edges[*].{param} ALL >= {value})
                    FILTER LENGTH(p.vertices) == {depth + 1}
                    RETURN  {{'vertex':v._id, '{param}': p.edges[*].{param}}}
                '''
            }

        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)
        if response.status_code == 201:
            result = response.json()['result']
            extra = response.json()['extra']
            self.getStats(graph, "queryDFS", extra)

            with open(f"results/results{graph}/queryDFS.json", "w") as file:
                json.dump(result, file, indent=4)
            return result
        else:
            print(f"Ошибка при выполнении запроса queryDFS: {response.text}")


    def queryFilterExtended(self, graph, collection, action, fieldName, value, degree):
        headers = {'Content-Type': 'application/json'}
        if graph == "Elliptic":
            data = {
                'query': f'''
                    FOR vertex IN {collection}
                         LET degree = LENGTH((FOR v, e IN 1..1 OUTBOUND vertex {action} 
                            FILTER v.{fieldName} >= {value} RETURN 1))
                         FILTER degree >= {degree}
                         RETURN {{'vertex' : vertex._id, 'degree' : degree}}
                        '''
            }
        elif graph == "RoadNet":
            data = {
                'query': f'''
                        FOR vertex IN {collection}
                            LET degree = LENGTH((FOR v, e IN 1..1 OUTBOUND vertex {action} 
                                                        RETURN 1))
                            FILTER degree >= {degree}
                            RETURN {{'vertex' : vertex._id, 'degree' : degree}}
                        '''
            }
        else:
            data = {
                'query': f'''
                           FOR vertex IN {collection}
                               LET degree = LENGTH((FOR v, e IN 1..1 OUTBOUND vertex {action} 
                                   FILTER e.{fieldName} >= {value} RETURN 1))
                               FILTER degree >= {degree}
                               RETURN {{'vertex' : vertex._id, 'degree' : degree}}
                       '''
            }

        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)
        if response.status_code == 201:
            result = response.json()['result']
            with open(f"results/results{graph}/queryFilterExtended.json", "w") as file:
                json.dump(result, file, indent=4)

            explanation = response.json()['extra']
            self.getStats(graph, "queryFilterExtended", explanation)

            return result
        else:
            print(f"Ошибка при выполнении запроса queryFilterExtended: {response.text}")

    def queryFilterSum(self, graph, collection, action, fieldName, value, sumValue):
        headers = {'Content-Type': 'application/json'}
        if graph == "Elliptic":
            data = {
                'query': f'''
                        FOR v IN {collection}
                            LET sum = (
                                FOR neighbor, e IN 1..1 OUTBOUND v._id {action}
                                    FILTER neighbor.{fieldName} > {value}
                                    RETURN neighbor.{fieldName}
                                )
                            LET totalSum = SUM(sum)
                            FILTER totalSum > {sumValue}
                            RETURN {{ 'vertex': v._id, 'sum': totalSum }}
                        '''
            }
        else:
            data = {
                'query': f'''
                        FOR u IN {collection}
                            LET sum = (
                                FOR v, e IN 1..1 OUTBOUND u._id {action}
                                    FILTER e.{fieldName} > {value}
                                    RETURN e.{fieldName}
                )
                            LET totalSum = SUM(sum)
                            FILTER totalSum > {sumValue}
                            RETURN {{
                                'vertex': u._id,
                                'sum': totalSum
                            }}
                    '''
            }

        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)
        if response.status_code == 201:
            result = response.json()['result']

            if result:
                with open(f"results/results{graph}/queryFilterSum.json", 'w') as file:
                    json.dump(result, file, indent=4)
            else:
                with open(f"results/results{graph}/queryFilterSum.json", 'w') as file:
                    json.dump("None", file, indent=4)
                print("Результаты запроса отсутствуют.")

            explanation = response.json()['extra']
            self.getStats(graph, "queryFilterSum", explanation)

            return result
        else:
            print(f"Ошибка при выполнении запроса queryFilterSum: {response.text}")

    def queryTriangles(self, graph, startVertex):

        headers = {'Content-Type': 'application/json'}
        data = {
            'query': f'''

        FOR v, e, p IN 3..3 ANY '{startVertex}' GRAPH {graph}
                    FILTER  p.vertices[0]._id == p.vertices[-1]._id
                    RETURN  {{'p': p.vertices[*]._id, 'e': p.edges[*]}}

            '''
        }

        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)

        if response.status_code == 201:
            result = response.json()['result']


            with open(f"results/results{graph}/queryTriangles.json", 'w') as file:
                json.dump(result, file, indent=4)

            explanation = response.json()['extra']
            self.getStats(graph, "queryTriangles", explanation)

            return result
        else:
            print(f"Ошибка при выполнении запроса queryTriangles: {response.text}")





if __name__ == "__main__":
    #config_path = sys.argv[1]

    #config_path = "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/QueryArango/configs/configElliptic.json"
    #config_path = "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/QueryArango/configs/configMooc.json"
    config_path = "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/QueryArango/configs/configRoadNet.json"
    #config_path = "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/QueryArango/configs/configStableCoin.json"


    with open(config_path, "r") as f:
        config = json.load(f)

    graph_name = config["graphName"]
    Query = ArangoDBQuery()

    with open(path + "stats/stats" + graph_name, 'w') as file:
        pass


    resultQueryFilter = Query.queryFilter(graph_name, config["queryFilter"]["collection"],
                                          config["queryFilter"]["fieldName"], config["queryFilter"]["value"])

    resultQueryFilterExtended = Query.queryFilterExtended(graph_name,
                                                          config["queryFilterExtended"]["collection"],
                                                          config["queryFilterExtended"]["edge"],
                                                          config["queryFilterExtended"]["fieldName"],
                                                          config["queryFilterExtended"]["value"],
                                                          config["queryFilterExtended"]["degree"])

    resultQueryBFS = Query.queryBFS(graph_name, config["queryBFS_DFS"]["depth"], config["queryBFS_DFS"]["startVertex"],
                                    config["queryBFS_DFS"]["fieldName"], config["queryBFS_DFS"]["value"])

    resultQueryDFS = Query.queryDFS(graph_name, config["queryBFS_DFS"]["depth"], config["queryBFS_DFS"]["startVertex"],
                                    config["queryBFS_DFS"]["fieldName"], config["queryBFS_DFS"]["value"])

    resultQueryFilterSum = Query.queryFilterSum(graph_name,
                                                          config["queryFilterSum"]["collection"],
                                                          config["queryFilterSum"]["action"],
                                                          config["queryFilterSum"]["fieldName"],
                                                          config["queryFilterSum"]["value"],
                                                          config["queryFilterSum"]["sumValue"])


    # resultQueryTriangles = Query.queryTriangles(graph_name, config["queryTriangles"]["startVertex"])

# data = {
#     'query': f'''
#         LET triangles = (
#             FOR v IN {collection}
#                 FOR w, e1 IN 1..1 ANY v GRAPH {graph}
#                     FOR x, e2 IN 1..1 ANY w GRAPH {graph}
#                         FILTER x._id != v._id
#                         LET triangle = [v._id, w._id, x._id]
#                         LET sortedTriangle = SORTED(triangle)
#                         RETURN {{ v: sortedTriangle[0], w: sortedTriangle[1], x: sortedTriangle[2] }}
#             )
#         RETURN  {{
#             'triangles': triangles,
#             'count': LENGTH(triangles)
#         }}
#     '''
# }
