from pyArango.collection import Collection, Edges
from pyArango.connection import *
from pyArango.graph import *
from readFiles.Conf import graph_config2, graph_config3, graph_config4, graph_config
import readFiles.readFilesMooc
import readFiles.readFilesElliptic
import readFiles


class GraphBD(object):
    # RoadNet dataset
    class NodeId(Collection):
        _fields = graph_config2["From"]["_fields"]

    class Action2(Edges):
        _fields = graph_config2["Action"]["_fields"]

    class roadNet(Graph):
        _edgeDefinitions = graph_config2["_edgeDefinitions"]
        _orphanedCollections = []

    # MOOC dataset
    class Target(Collection):
        _fields = graph_config["To"]["_fields"]

    class User(Collection):
        _fields = graph_config["From"]["_fields"]

    class Action(Edges):
        _fields = graph_config["Action"]["_fields"]

    class MoocGraph(Graph):
        _edgeDefinitions = graph_config["_edgeDefinitions"]
        _orphanedCollections = []

    #Elliptic dataset
    class txId(Collection):
        _fields = graph_config3["From"]["_fields"]

    class Action3(Edges):
        pass
        # _fields = graph_config3["Action3"]["_fields"]

    class Elliptic(Graph):
        _edgeDefinitions = graph_config3["_edgeDefinitions"]
        _orphanedCollections = []

    # Coin dataset
    class Action4(Edges):
        _fields = graph_config4["Action4"]["_fields"]

    class Address(Collection):
        _fields = graph_config4["From"]["_fields"]

    class stableCoin(Graph):
        _edgeDefinitions = graph_config4["_edgeDefinitions"]
        _orphanedCollections = []

    def __init__(self, conf, conns, actionFeatures, nodeFeatures):
        self.conn = Connection(username="root", password="1")
        self.db = self.conn["_system"]
        self.conf = conf

        name_to = conf["name_to"]
        name_from = conf["name_from"]
        name_action = conf["name_action"]

        graph_name = conf["graphName"]
        if self.db.hasGraph(graph_name):
            print("Graph already exists")
            graph = self.db.graphs[graph_name]
            graph.delete()

        g = self.db.createGraph(graph_name)

        for i, (nodeFrom, nodeTo) in enumerate(conns):

            from_collection = self.db.collections[name_from]
            from_vertex = from_collection.fetchFirstExample({"_key": nodeFrom})
            if from_vertex:
                a = from_vertex[0]
            else:
                if len(nodeFeatures) != 0:
                    if nodeFrom in nodeFeatures:
                        fieldsNode = conf["From"]["_fields"]
                        data = {}
                        nodeFeature = nodeFeatures[nodeFrom]
                        data["_key"] = nodeFrom
                        for j, field in enumerate(fieldsNode):
                            data[field] = nodeFeature[j]

                        a = g.createVertex(name_from, data)
                    else:
                        a = g.createVertex(name_from, { "_key": str(nodeFrom)})
                else:
                    a = g.createVertex(name_from, {"_key": str(nodeFrom)})

            to_collection = self.db.collections[name_to]
            to_vertex = to_collection.fetchFirstExample({"_key": nodeTo})
            # b = to_vertex[0]
            if to_vertex:
                b = to_vertex[0]
            else:
                if len(nodeFeatures) != 0:
                    if nodeTo in nodeFeatures:
                        fieldsNode = conf["From"]["_fields"]
                        data = {}
                        data["_key"] = nodeTo
                        nodeFeature = nodeFeatures[nodeTo]
                        for j, field in enumerate(fieldsNode):
                            data[field] = nodeFeature[j]
                        b = g.createVertex(name_to, data)
                    else:
                        b = g.createVertex(name_to, {"_key": str(nodeTo)})
                else:

                    b = g.createVertex(name_to, { "_key": str(nodeTo)})

            a.save()
            b.save()

            if len(actionFeatures) != 0:
                fieldsAction = conf[name_action]["_fields"]
                data = {}
                actionFeature = actionFeatures[i]
                for j, field in enumerate(fieldsAction):
                    data[field] = actionFeature[j]
                g.link(name_action, a, b, data)
            else:
                g.link(name_action, a, b, {"type": 'connection'})


if __name__ == "__main__":

    # readFilesMooc readFilesElliptic
    #config, connections, actionFeatures, nodeFeature = readFiles.readFilesMooc.readFilesMooc()
    config, connections, actionFeatures, nodeFeature = readFiles.readFilesElliptic.readFilesElliptic()
    print("all Data were read")
    GraphBD(config, connections, actionFeatures, nodeFeature)
    print(f"Successfully created graph database '{graph_config3['graphName']}'")



