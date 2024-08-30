from pyArango.collection import Collection, Field, Edges
from pyArango.graph import *

graph_config = {
    "connections" : ("action_id", "user_id", "target_id"),
    "graphName" : "MoocGraph",
    "_edgeDefinitions": (EdgeDefinition('Action',
                                        fromCollections=["User"],
                                        toCollections=["Target"]),),
    "name_to" : "Target",
    "name_from" : "User",
    "name_action" : "Action",
    "_orphanedCollections": [],

    "Action": {
        "_fields": {
            "action_id": Field(),
            "timestamp": Field(),
            "label": Field(),
            "feature0": Field(),
            "feature1": Field(),
            "feature2": Field(),
            "feature3": Field()
        }
    },
    "To": {
        "_fields": {
            "target_id": Field()
        }
    },
    "From": {
        "_fields": {
            "user_id": Field()
        }
    }
}


graph_config2 = {
    "graphName" : "roadNet",
    "_edgeDefinitions": (EdgeDefinition('Action2',
                                        fromCollections=["NodeId"],
                                        toCollections=["NodeId"]),),

    "name_to": "NodeId",
    "name_from": "NodeId",
    "name_action": "Action2",
    "Action": {
        "_fields": {
            "number": Field()
        }
    },
    "To": {
        "_fields": {
            "NodeId": Field()
        }
    },
    "From": {
        "_fields": {
            "NodeId": Field()
        }
    }
}

graph_config3 = {
    "graphName" : "Elliptic",
    "_edgeDefinitions": (EdgeDefinition('Action3',
                                        fromCollections=["txId"],
                                        toCollections=["txId"]),),
    "Action3": {
        # "_fields": {
        #     "number": Field()
        # }
    },
    "name_to": "txId",
    "name_from": "txId",
    "name_action": "Action3",
    "From": {
        "_fields": {
            "timestamp": Field(),
            "Local_feature_1": Field(),
            "Aggregate_feature_1": Field(),
            "in_txs_degree": Field(),
            "out_txs_degree": Field(),
            "total_BTC": Field(),
            "fees": Field(),
            "size": Field(),
            "num_input_addresses": Field(),
            "num_output_addresses": Field(),
            "in_BTC_min": Field(),
            "in_BTC_max": Field(),
            "in_BTC_mean": Field(),
            "in_BTC_median": Field(),
            "in_BTC_total": Field(),
            "out_BTC_min": Field(),
            "out_BTC_max": Field(),
            "out_BTC_mean": Field(),
            "out_BTC_median": Field(),
            "out_BTC_total": Field(),
            "class": Field()
        }
    },
}

graph_config4 = {
    "graphName" : "stableCoin",
    "_edgeDefinitions": (EdgeDefinition('Action4',
                                        fromCollections=["Address"],
                                        toCollections=["Address"]),),

    "name_to": "Address",
    "name_from": "Address",
    "name_action": "Action4",
    "Action4" : {
        "_fields": {
            "blockNumber": Field(),
            "transIndex": Field(),
            "timestamp": Field(),
            "contractAddress" : Field(),
            "value": Field(),

        }
    },
    "To": {
        "_fields": {
            "AddressID": Field()
        }
    },
    "From": {
        "_fields": {
            "AddressID": Field()
        }
    }


}
