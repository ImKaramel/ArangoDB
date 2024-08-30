#Elliptic
from Conf import graph_config3


def readFilesElliptic():
    pathEllipticFeat = readEllipticAction(
        "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/arangoLoadData/data/EllipticDataset/txs_features.csv")
    pathEllipticConns = readEllipticConns(
        "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/arangoLoadData/data/EllipticDataset/txs_edgelist.csv")

    pathNodeField = readEllipticClass(
        "/Users/assistentka_professora/Desktop/ArangoDB/ArangoDB/arangoLoadData/data/EllipticDataset/txs_classes.csv", pathEllipticFeat)

    return graph_config3, pathEllipticConns, [], pathNodeField

def convert_to_float(value):
    try:
        return float(value)
    except ValueError:
        return value
def readEllipticAction(file_path):
    data = {}
    with open(file_path, 'r') as file:
        next(file)
        for line in file:
            str = line.strip().split(',')
            data[str[0]] = [float(str[1]), float(str[2]), float(str[95]),  (str[167]),
                             convert_to_float(str[168]), convert_to_float(str[169]), convert_to_float(str[170]), convert_to_float(str[171]),
                             convert_to_float(str[172]), convert_to_float(str[173]),
                            convert_to_float(str[174]), convert_to_float(str[175]), convert_to_float(str[176]),
                             convert_to_float(str[177]), convert_to_float(str[178]),
                             convert_to_float(str[179]), convert_to_float(str[180]), convert_to_float(str[181]), convert_to_float(str[182]),
                            convert_to_float(str[183])
                    ]

    return data

def readEllipticConns(file_path):
    data = []
    nodes = set({})
    with open(file_path, 'r') as file:
        next(file)
        for line in file:
            str = line.strip().split(',')
            data.append((str[0], str[1]))
            nodes.add(str[0])
            nodes.add(str[1])
    return data

def readEllipticClass(file_path, my_dict):
    with open(file_path, 'r') as file:
        next(file)
        for line in file:
            str = line.strip().split(',')
            my_dict[str[0]].append(int(str[1]))
            # print(my_dict[str[0]])
    return my_dict