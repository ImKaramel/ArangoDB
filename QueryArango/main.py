import json
import sys
import requests
import tracemalloc
import time


path = "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/QueryArango/"

class ArangoDBQuery:
    def __init__(self):
        self.url = 'http://localhost:8529'
        self.db_name = '_system'
        self.username = 'root'
        self.password = '1'
        self.auth = (self.username, self.password)
        self.endpoint = f'{self.url}/_db/{self.db_name}/_api/cursor'

    def getStats(self, graph, nameQuery, time, memory):
    # def getStats(self, graph, nameQuery, explanation):
        # time = explanation["stats"]["executionTime"]
        # memory = explanation["stats"]["peakMemoryUsage"]

        with open(path + "stats/stats" + graph, "a") as file:
            file.write(nameQuery + "\n")
            file.write("")
            file.write("" + "{:.6f}".format(memory) + " Kb" + "\n")
            file.write("{:.6f}".format(time) + " s" + "\n\n\n")

#_timestamp
    def queryFilter(self, graph, collection, fieldName, value):
        headers = {'Content-Type': 'application/json'}
        data = {'query':
                    f'''FOR v IN {collection} 
                        OPTIONS {{useCache: false, useIndex: "_{fieldName}"}} 
                        FILTER v.{fieldName} >= {value} 
                        RETURN {{"id": v._key, "{fieldName}": v.{fieldName}
                        }}''',
                }
        start_time = time.time()
        tracemalloc.start()
        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)
        end_time = time.time()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        if response.status_code == 201:
            result = response.json()['result']
            self.getStats(graph, "queryFilter", end_time - start_time, top_stats[0].size / 1024)
            # extra = response.json()['extra']
            # self.getStats(graph, "queryFilter", extra)
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
                            OPTIONS {{"order": "bfs", useCache: false, useIndex: "_{param}"}} 
                            FILTER (p.vertices[*].{param} ALL >= {value})
                            RETURN  {{'vertex':v._key, '{param}': p.vertices[*].{param}}}
                        '''
            }
        elif graph == "RoadNet":
            data = {
                'query': f'''
                                FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
                                OPTIONS {{"order": "bfs"}}
                                RETURN  {{'vertex':v._key}}
                            '''
            }
        else:
            data = {
                'query': f'''
                    FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
                    OPTIONS {{"order": "bfs", useCache: false, useIndex: "_{param}"}} 
                    FILTER (p.edges[*].{param} ALL >= {value})
                    RETURN  {{'vertex':v._key, '{param}': p.edges[*].{param}}}
                '''
            }
        start_time = time.time()
        tracemalloc.start()
        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)

        end_time = time.time()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        if response.status_code == 201:
            result = response.json()['result']
            self.getStats(graph, "queryBFS", end_time - start_time, top_stats[0].size / 1024)
            # extra = response.json()['extra']
            # self.getStats(graph, "queryBFS", extra)
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
                            OPTIONS {{"order": "dfs", useCache: false, useIndex: "_{param}"}} 
                            FILTER  (p.vertices[*].{param} ALL >= {value})
                            RETURN  {{'vertex':v._key, '{param}': p.vertices[*].{param}}}
                        '''
            }
        elif graph == "RoadNet":
            data = {
                'query': f'''
                                       FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
                                       OPTIONS {{"order": "dfs"}}
                                       RETURN  {{'vertex':v._key}}
                                   '''
            }
        else:
            data = {
                'query': f'''
                    FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
                    OPTIONS {{"order": "dfs", useCache: false, useIndex: "_{param}"}} 
                    FILTER (p.edges[*].{param} ALL > {value})
                    RETURN  {{'vertex':v._key, '{param}': p.edges[*].{param}}}
                '''
            }
        start_time = time.time()
        tracemalloc.start()
        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)

        end_time = time.time()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        if response.status_code == 201:
            result = response.json()['result']
            self.getStats(graph, "queryDFS", end_time - start_time, top_stats[0].size / 1024)
            # extra = response.json()['extra']
            # self.getStats(graph, "queryDFS", extra)

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
                    OPTIONS {{ useCache: false, useIndex: "_{fieldName}"}} 
                         LET degree = LENGTH((FOR v, e IN 1..1 OUTBOUND vertex {action} 
                            FILTER v.{fieldName} >= {value} RETURN 1))
                         FILTER degree >= {degree}
                         RETURN {{'vertex' : vertex._key, 'degree' : degree}}
                        '''
            }
        elif graph == "RoadNet":
            data = {
                'query': f'''
                        FOR vertex IN {collection}
                            LET degree = LENGTH((FOR v, e IN 1..1 OUTBOUND vertex {action} 
                                                        RETURN 1))
                            FILTER degree >= {degree}
                            RETURN {{'vertex' : vertex._key, 'degree' : degree}}
                        '''
            }
        else:
            data = {
                'query': f'''
                           FOR vertex IN {collection}
                           OPTIONS {{ useCache: false, useIndex: "_{fieldName}"}} 
                               LET degree = LENGTH((FOR v, e IN 1..1 OUTBOUND vertex {action} 
                                   FILTER e.{fieldName} >= {value} RETURN 1))
                               FILTER degree >= {degree}
                               RETURN {{'vertex' : vertex._key, 'degree' : degree}}
                       '''
            }

        start_time = time.time()
        tracemalloc.start()
        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)
        end_time = time.time()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        if response.status_code == 201:
            result = response.json()['result']
            with open(f"results/results{graph}/queryFilterExtended.json", "w") as file:
                json.dump(result, file, indent=4)
            self.getStats(graph, "queryFilterExtended", end_time - start_time, top_stats[0].size / 1024)
            # explanation = response.json()['extra']
            # self.getStats(graph, "queryFilterExtended", explanation)

            return result
        else:
            print(f"Ошибка при выполнении запроса queryFilterExtended: {response.text}")

    def queryFilterSum(self, graph, collection, action, fieldName, value, sumValue):
        headers = {'Content-Type': 'application/json'}
        if graph == "Elliptic":
            data = {
                'query': f'''
                        FOR v IN {collection}
                        OPTIONS {{ useCache: false, useIndex: "_{fieldName}"}} 
                            LET sum = (
                                FOR neighbor, e IN 1..1 OUTBOUND v._key {action}
                                    FILTER neighbor.{fieldName} > {value}
                                    RETURN neighbor.{fieldName}
                                )
                            LET totalSum = SUM(sum) 
                            RETURN {{ 'vertex': v._key, 'sum': totalSum }}
                        '''
            }
        else:
            data = {
                'query': f'''
                        FOR u IN {collection}
                        OPTIONS {{ useCache: false, useIndex: "_{fieldName}"}} 
                            LET sum = (
                                FOR v, e IN 1..1 OUTBOUND u {action}
                                    FILTER e.{fieldName} > {value}
                                    RETURN e.{fieldName}
                )
                            LET totalSum = SUM(sum)
                            RETURN {{
                                'vertex': u._key,
                                'sum': totalSum
                            }}
                    '''
            }

        start_time = time.time()
        tracemalloc.start()
        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)
        end_time = time.time()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        if response.status_code == 201:
            result = response.json()['result']

            if result:
                with open(f"results/results{graph}/queryFilterSum.json", 'w') as file:
                    json.dump(result, file, indent=4)
            else:
                with open(f"results/results{graph}/queryFilterSum.json", 'w') as file:
                    json.dump("None", file, indent=4)


            self.getStats(graph, "queryFilterSum", end_time - start_time, top_stats[0].size / 1024)
            # explanation = response.json()['extra']
            # self.getStats(graph, "queryFilterSum", explanation)

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

    def queryShortPath(self, graph, fromVertex, toVertex):
        headers = {'Content-Type': 'application/json'}
        data = {
            'query': f'''
            
                FOR v, e IN OUTBOUND SHORTEST_PATH '{fromVertex}' TO '{toVertex}' GRAPH '{graph}'
                OPTIONS {{ useCache: false}} 
                        RETURN [v._key, e._key]
                                '''
        }

        start_time = time.time()
        tracemalloc.start()
        response = requests.post(self.endpoint, headers=headers, json=data, auth=self.auth)
        end_time = time.time()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        if response.status_code == 201:
            result = response.json()['result']

            if result:
                with open(f"results/results{graph}/queryShortPath.json", 'w') as file:
                    json.dump(result, file, indent=4)
            else:
                with open(f"results/results{graph}/queryShortPath.json", 'w') as file:
                    json.dump("None", file, indent=4)
                print("Результаты запроса отсутствуют.")
            self.getStats(graph, "queryShortPath", end_time - start_time, top_stats[0].size / 1024)
            # explanation = response.json()['extra']
            # self.getStats(graph, "queryShortPath", explanation)

            return result
        else:
            print(f"Ошибка при выполнении запроса queryShortPath: {response.text}")

if __name__ == "__main__":
    #config_path = sys.argv[1]

    #config_path = "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/QueryArango/configs/configElliptic.json"
    #config_path = "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/QueryArango/configs/configMooc.json"
    #config_path = "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/QueryArango/configs/configRoadNet.json"
    config_path = "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/QueryArango/configs/configStableCoin.json"

    with open(config_path, "r") as f:
        config = json.load(f)

    graph_name = config["graphName"]
    Query = ArangoDBQuery()

    # with open(path + "stats/stats" + graph_name, 'w') as file:
    #     pass

    # resultQueryFilter = Query.queryFilter(graph_name, config["queryFilter"]["collection"],
    #                                       config["queryFilter"]["fieldName"], config["queryFilter"]["value"])


    # resultQueryFilterExtended = Query.queryFilterExtended(graph_name,
    #                                                       config["queryFilterExtended"]["collection"],
    #                                                       config["queryFilterExtended"]["edge"],
    #                                                       config["queryFilterExtended"]["fieldName"],
    #                                                       config["queryFilterExtended"]["value"],
    #                                                       config["queryFilterExtended"]["degree"])


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


    resultQueryShortPath = Query.queryShortPath(graph_name,
                                                config["queryShortPath"]["fromVertex"],
                                                config["queryShortPath"]["toVertex"],
                                                )

    #resultQueryTriangles = Query.queryTriangles(graph_name, config["queryTriangles"]["startVertex"])

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


#docker run -d --name scylladb -p 9042:9042 -v /Users/assistentka_professora/Desktop/Scylla/var/lib/scylla:/var/lib/scylla scylladb/scylla --smp 1
