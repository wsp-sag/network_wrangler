class Client(object):
    """
    MTC specific wrangler variables
    """

    CRS = 4326#"EPSG:4326"

    NODE_FOREIGN_KEY = "model_node_id"
    LINK_FOREIGN_KEY = ["A", "B"]

    SEARCH_BREADTH = 5
    MAX_SEARCH_BREADTH = 10
    SP_WEIGHT_FACTOR = 100

    # http://bayareametro.github.io/travel-model-two/input/#county-node-numbering-system
    MANAGED_LANES_NODE_ID_SCALAR = 4500000
    MANAGED_LANES_LINK_ID_SCALAR = 10000000

    SELECTION_REQUIRES = ["link"]

    UNIQUE_LINK_KEY = "model_link_id"
    UNIQUE_NODE_KEY = "model_node_id"
    UNIQUE_MODEL_LINK_IDENTIFIERS = ["model_link_id"]
    UNIQUE_NODE_IDENTIFIERS = ["model_node_id"]

    UNIQUE_SHAPE_KEY = "id"

    MANAGED_LANES_REQUIRED_ATTRIBUTES = [
        "A",
        "B",
        "model_link_id",
        "locationReferences",
    ]

    KEEP_SAME_ATTRIBUTES_ML_AND_GP = [
        "distance",
        "bike_access",
        "drive_access",
        "transit_access",
        "walk_access",
        "maxspeed",
        "name",
        "oneway",
        "ref",
        "roadway",
        "length",
        "segment_id",
    ]

    MANAGED_LANES_SCALAR = 4500000

    MODES_TO_NETWORK_LINK_VARIABLES = {
        "drive": "drive_access",
        "transit": "transit_access",
        "walk": "walk_access",
        "bike": "bike_access",
    }

    MODES_TO_NETWORK_NODE_VARIABLES = {
        "drive": "drive_access",
        "transit": "transit_access",
        "walk": "walk_access",
        "bike": "bike_access",
    }


    def __init__(
        self, **kwargs
    ):
        """
        """
        __dict__.update(kwargs)
