import json


def read_Road_data(file_path):
    data = []
    nodes = set({})
    with open(file_path, "r") as f:
        next(f)
        next(f)
        next(f)
        next(f)
        for line in f:
            fromNode, toNode = line.strip().split()
            data.append((fromNode, toNode))
            nodes.add(fromNode)
            nodes.add(toNode)
    return nodes, data


if __name__ == "__main__":
    filePath = "/Users/assistentka_professora/Desktop/ArangoDB/arangoLoadData/data/roadNet-CA.txt"
    nodes, conns = read_Road_data(filePath)
    conns = read_Road_data(filePath)
    print("all data were read")

    nodes_dicts = []
    print(len(nodes))
    for node in nodes:
        nodes_dicts.append({"_id": node, "_key": node})
    print("all nodes were read")

    action = []
    for conn in conns:
        action.append({"_from": f'NodeId/{conn[0]}', "_to": f'NodeId/{conn[1]}'})

    print("all actions were read")

    with open('nodesRoad.json', 'w') as f:
        json.dump(nodes_dicts, f)
        f.write('')

    print("json for nodes was created")

    with open('actionRoad.json', 'w') as f:
        json.dump(action, f)
        f.write('')

    print("json for actions were created")

    print("allRight")