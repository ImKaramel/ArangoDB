import json

from Conf import graph_config


def readFilesMooc():
    actions = "/Users/assistentka_professora/Desktop/ArangoDB/arangoLoadData/data/MOOC/act-mooc/mooc_actions.tsv"
    label = "/Users/assistentka_professora/Desktop/ArangoDB/arangoLoadData/data/MOOC/act-mooc/mooc_action_labels.tsv"
    actionFeat = ("/Users/assistentka_professora/Desktop/ArangoDB/arangoLoadData/data/MOOC/"
                  "act-mooc/mooc_action_features.tsv")

    conns, timest, user, tar = read_data_conns(actions)
    labels = read_label_data(label)
    actionFeatures = read_feature_data(actionFeat, timest, labels)

    # nodes_dicts = []
    # print(tar)
    # for node in tar:
    #     nodes_dicts.append({"_id": node})
    # print("all nodes were read")
    #
    # with open('nodesTarget.json', 'w') as f:
    #     json.dump(nodes_dicts, f)
    #     f.write('')
    #
    # print("json for nodes was created")
    #
    # for node in user:
    #     nodes_dicts.append({"_id": node})
    # print("all user nodes were read")
    #
    # with open('nodesUser.json', 'w') as f:
    #     json.dump(nodes_dicts, f)
    #     f.write('')
    #
    # print("json for user nodes was created")
    #
    # action = []
    # for conn in conns:
    #     action.append({"_from": f'Address/{conn[0]}', "_to": f'Address/{conn[1]}', "time_stamp": conn[2],  "value": conn[3]})
    #
    # print(len(action))
    #
    # print("all actions were read")

    return graph_config, conns, actionFeatures, []


def read_data_conns(file_path):
    data = []
    timestamps = []
    tar = set({})
    user = set({})
    with open(file_path, "r") as f:
        next(f)
        for line in f:
            action_id, user_id, target_id, timestamp = line.strip().split()
            data.append((user_id, target_id))
            tar.add(target_id)
            user.add(user_id)
            timestamps.append(float(timestamp))
    return data, timestamps, user, tar


def read_feature_data(file_path, timestamps, labels):
    data = []
    with open(file_path, "r") as f:
        next(f)
        for i, line in enumerate(f):
            action_id, feature0, feature1, feature2, feature3 = line.strip().split()
            timestamp = timestamps[i]
            label = labels[i]
            data.append((action_id, timestamp, label, float(feature0), float(feature1),
                         float(feature2), float(feature3)))
    return data


def read_label_data(file_path):
    data = []
    with open(file_path, "r") as f:
        next(f)
        for line in f:
            action_id, label = line.strip().split()
            data.append(int(label))
    return data


# if __name__ == "__main__":
#     readFilesMooc()
#     filePath = "/Users/assistentka_professora/Desktop/ArangoDB/arangoLoadData/data/ERC20-stablecoins/token_transfers_V2.0.0.csv"
#     nodes, conns = readCoinData(filePath)
#     print("all data were read")
#
#     # nodes_dicts = []
#
#     # for node in nodes:
#     #     nodes_dicts.append({"_key": node})
#     # print("all nodes were read")
#
#     action = []
#     for conn in conns:
#         action.append({"_from": f'Address/{conn[0]}', "_to": f'Address/{conn[1]}', "time_stamp": conn[2],  "value": conn[3]})
#
#     print(len(action))
#
#     print("all actions were read")
#
    # with open('nodes.json', 'w') as f:
    #     json.dump(nodes_dicts, f)
    #     f.write('')
    #
    # print("json for nodes was created")
#
#     with open('action.json', 'w') as f:
#         json.dump(action, f)
#         f.write('')
#
#     print("json for actions were created")
#
#     print("allRight")



# def readEllipticAddress(file_path, my_dict):
#     with open(file_path, 'r') as file:
#         next(file)
#         for line in file:
#             data = line.strip().split(',')
#             key = data[0]
#             value = data[1]
#             if key not in my_dict:
#                 print("here")
#                 my_dict[key] = []
#             my_dict[key].append(value)
#     return my_dict





