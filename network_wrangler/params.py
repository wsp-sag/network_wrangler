"""Parameters for Network Wrangler."""

from dataclasses import dataclass, field
from typing import List, Literal

# ---------------------------------------------------------------------
# ------ GEOGRAPHIC DATA PARAMS ------
# ---------------------------------------------------------------------

# Projected CRS to use when calculating distances
# UTM zone 10N (EPSG:26910): Covers parts of the West Coast (e.g., parts of California)
# UTM zone 11N (EPSG:26911): Covers further inland West Coast areas (e.g., Nevada)
# UTM zone 12N (EPSG:26912): Covers much of the Mountain States
# UTM zone 13N (EPSG:26913): Includes states like Colorado and Kansas
# UTM zone 14N (EPSG:26914): Used in the Central states such as Oklahoma
# UTM zone 15N (EPSG:26915): Covers regions around the Mississippi River
# UTM zone 16N (EPSG:26916): Encompasses areas like Tennessee
# UTM zone 17N (EPSG:26917): Covers parts of the Eastern states
# UTM zone 18N (EPSG:26918): Includes parts of the East Coast like New York
# UTM zone 19N (EPSG:26919): Extends to the northeastern US, including states like Maine
METERS_CRS = 26915

# Lat/Long CRS to use
LAT_LON_CRS = 4326

# ---------------------------------------------------------------------
# ------ ROADWAY DATA PARAMS ------
# ---------------------------------------------------------------------

ROAD_SHAPE_ID_SCALAR = 1000


@dataclass
class NodesParams:
    """Parameters for RoadNodesTable."""

    primary_key: str = field(default="model_node_id")
    _addtl_unique_ids: list[str] = field(default_factory=lambda: ["osm_node_id"])
    _addtl_explicit_ids: list[str] = field(default_factory=lambda: [])
    source_file: str = field(default=None)
    table_type: Literal["nodes"] = field(default="nodes")
    x_field: str = field(default="X")
    y_field: str = field(default="Y")

    @property
    def geometry_props(self) -> List[str]:
        """List of geometry properties."""
        return [self.x_field, self.y_field, "geometry"]

    @property
    def idx_col(self) -> str:
        """Column to make the index of the table."""
        return self.primary_key + "_idx"

    @property
    def unique_ids(self) -> List[str]:
        """List of unique ids for the table."""
        _uids = self._addtl_unique_ids + [self.primary_key]
        return list(set(_uids))

    @property
    def explicit_ids(self) -> List[str]:
        """List of columns that can be used to easily find specific records the table."""
        _eids = self._addtl_unique_ids + self.unique_ids
        return list(set(_eids))

    @property
    def display_cols(self) -> List[str]:
        """Columns to display in the table."""
        return self.explicit_ids


@dataclass
class LinksParams:
    """Parameters for RoadLinksTable."""

    primary_key: str = field(default="model_link_id")
    _addtl_unique_ids: list[str] = field(default_factory=lambda: [])
    _addtl_explicit_ids: list[str] = field(default_factory=lambda: ["osm_link_id"])
    from_node: str = field(default="A")
    to_node: str = field(default="B")
    fk_to_shape: str = field(default="shape_id")
    table_type: Literal["links"] = field(default="links")
    source_file: str = field(default=None)
    modes_to_network_link_variables: dict = field(
        default_factory=lambda: MODES_TO_NETWORK_LINK_VARIABLES
    )

    @property
    def idx_col(self):
        """Column to make the index of the table."""
        return self.primary_key + "_idx"

    @property
    def fks_to_nodes(self):
        """Foreign keys to nodes in the network."""
        return [self.from_node, self.to_node]

    @property
    def unique_ids(self) -> List[str]:
        """List of unique ids for the table."""
        _uids = self._addtl_unique_ids + [self.primary_key]
        return list(set(_uids))

    @property
    def explicit_ids(self) -> List[str]:
        """List of columns that can be used to easily find specific row sin the table."""
        return list(set(self.unique_ids + self._addtl_explicit_ids))

    @property
    def display_cols(self) -> List[str]:
        """List of columns to display in the table."""
        _addtl = ["lanes"]
        return list(set(self.explicit_ids + self.fks_to_nodes + _addtl))


@dataclass
class ShapesParams:
    """Parameters for RoadShapesTable."""

    primary_key: str = field(default="shape_id")
    _addtl_unique_ids: list[str] = field(default_factory=lambda: [])
    table_type: Literal["shapes"] = field(default="shapes")
    source_file: str = field(default=None)

    @property
    def idx_col(self) -> str:
        """Column to make the index of the table."""
        return self.primary_key + "_idx"

    @property
    def unique_ids(self) -> list[str]:
        """List of unique ids for the table."""
        return list(set(self._addtl_unique_ids.append(self.primary_key)))


LINK_ML_OFFSET_METERS = 10

MODES_TO_NETWORK_LINK_VARIABLES = {
    "drive": ["drive_access"],
    "bus": ["bus_only", "drive_access"],
    "rail": ["rail_only"],
    "transit": ["bus_only", "rail_only", "drive_access"],
    "walk": ["walk_access"],
    "bike": ["bike_access"],
}

# ---------------------------------------------------------------------
# ------ TRANSIT PARAMS ------
# ---------------------------------------------------------------------

# Default initial scalar value to add to duplicated shape_ids to create a new shape_id
TRANSIT_SHAPE_ID_SCALAR = 1000000

# Default initial scalar value to add to node id to create a new stop_id
TRANSIT_STOP_ID_SCALAR = 1000000

# ---------------------------------------------------------------------
# ------ SCENARIO PARAMS ------
# ---------------------------------------------------------------------

BASE_SCENARIO_SUGGESTED_PROPS = [
    "road_net",
    "transit_net",
    "applied_projects",
    "conflicts",
]
TRANSIT_CARD_TYPES = ["transit_property_change"]
ROADWAY_CARD_TYPES = [
    "roadway_deletion",
    "roadway_addition",
    "roadway_property_change",
]
SECONDARY_TRANSIT_CARD_TYPES = ["roadway_deletion"]

# ---------------------------------------------------------------------
# ------ SEARCH PARAMS ------
# ---------------------------------------------------------------------

DEFAULT_SEARCH_MODES = ["drive"]

"""
(int): default for initial number of links from name-based
    selection that are traveresed before trying another shortest
    path when searching for paths between A and B node
"""
DEFAULT_SEARCH_BREADTH = 5

"""
(int): default for maximum number of links traversed between
    links that match the searched name when searching for paths
    between A and B node
"""
DEFAULT_MAX_SEARCH_BREADTH = 10

"""
Union(int, float)): default penalty assigned for each
    degree of distance between a link and a link with the searched-for
    name when searching for paths between A and B node
"""
DEFAULT_SP_WEIGHT_FACTOR = 100

"""
(str): default column to use as weights in the shortest path calculations.
"""
DEFAULT_SP_WEIGHT_COL = "i"

"""Default timespan for scoped values."""
DEFAULT_TIMESPAN = ["00:00", "24:00"]

"""Default category for scoped values."""
DEFAULT_CATEGORY = "any"

"""Read sec / MB - WILL DEPEND ON SPECIFIC COMPUTER
"""
EST_PD_READ_SPEED = {
    "csv": 0.03,
    "parquet": 0.005,
    "geojson": 0.03,
    "json": 0.15,
    "txt": 0.04,
}

"""
(list(str)): list of attributes
to copy from a general purpose lane to managed lane so long as a ML_<prop_name> doesn't exist.
"""
COPY_FROM_GP_TO_ML = [
    "ref",
    "roadway",
    "access",
    "distance",
    "bike_access",
    "drive_access",
    "walk_access",
    "bus_only",
    "rail_only",
]


"""
(list(str)): list of attributes copied from GP lanes to access and egress dummy links.
"""
COPY_TO_ACCESS_EGRESS = [
    "ref",
    "ML_access",
    "ML_drive_access",
    "ML_bus_only",
    "ML_rail_only",
]

"""
(list(str)): list of attributes
that must be provided in managed lanes
"""
MANAGED_LANES_REQUIRED_ATTRIBUTES = [
    "A",
    "B",
    "model_link_id",
]

"""
scalar value added to the general purpose lanes' `model_link_id` when creating
    an associated link for a parallel managed lane
"""
MANAGED_LANES_LINK_ID_SCALAR = 1000000

"""
scalar value added to the general purpose lanes' `model_node_id` when creating
    an associated node for a parallel managed lane
"""
MANAGED_LANES_NODE_ID_SCALAR = 500000
