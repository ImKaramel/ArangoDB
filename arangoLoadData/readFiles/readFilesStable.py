import json


def readFilesStable():
    filePath = "/Users/assistentka_professora/Desktop/ArangoDB/arangoLoadData/data/ERC20-stablecoins/token_transfers_V2.0.0.csv"
    conns, actFeatures = readCoinData(filePath)
    return conns, actFeatures

def readCoinData(file_path):
    pattern = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    data = set({})
    conns = []
    with open(file_path, "r") as f:
        next(f)
        for line in f:
            a1, a2, fr, to, tms, contract, value = line.strip().split(',')
            if contract == pattern:
                conns.append((fr, to, float(tms), float(value)))
                data.add(fr)
                data.add(to)

    return data, conns

if __name__ == "__main__":
    print("here")
    filePath = "/Users/assistentka_professora/Desktop/ArangoDB/arangoLoadData/data/ERC20-stablecoins/token_transfers_V2.0.0.csv"
    filename = "actionCoin.json"
    with open(filename, "r") as file:
        data = json.load(file)
        count = len(data)
        print("Количество записей в файле:", count)
    print("here2")
    nodes, conns = readCoinData(filePath)
    print("all data were read")

    nodes_dicts = []

    for node in nodes:
        nodes_dicts.append({"_id": node, "_key": node})
    print("all nodes were read")
    #
    action = []
    for conn in conns:
        action.append({"_from": f'Address/{conn[0]}', "_to": f'Address/{conn[1]}', "time_stamp": conn[2],  "value": conn[3]})

    print(len(action))

    print("all actions were read")
    with open('nodesCoin.json', 'w') as f:
        json.dump(nodes_dicts, f)
        f.write('')

    print("json for nodes was created")

    with open('actionCoin.json', 'w') as f:
        json.dump(action, f)
        f.write('')

    print("json for actions were created")

    print("allRight")



