import copy

import numpy as np
import pandas as pd
from geopandas import GeoDataFrame

from .selection import filter_links_nodes_by_mode
from .graph import links_nodes_to_ox_graph, shortest_path
from ..logger import WranglerLogger


"""
(str): default column to use as weights in the shortest path calculations.
"""
SP_WEIGHT_COL = "i"

"""
Union(int, float)): default penalty assigned for each
    degree of distance between a link and a link with the searched-for
    name when searching for paths between A and B node
"""
SP_WEIGHT_FACTOR = 100

"""
(int): default for initial number of links from name-based
    selection that are traveresed before trying another shortest
    path when searching for paths between A and B node
"""
SEARCH_BREADTH = 5

"""
(int): default for maximum number of links traversed between
    links that match the searched name when searching for paths
    between A and B node
"""
MAX_SEARCH_BREADTH = 10


class SegmentFormatError(Exception):
    pass


class SegmentSelectionError(Exception):
    pass


class Segment:
    """Segment is a contiguous section of the roadway network defined by a start and end node and
    a facility of one or more names.

    Segments are defined by a selection dictionary and then searched for on the network using
    a shortest path graph search.
    """

    def __init__(self, net, segment_dict={}):
        self.net = net
        self.selection_dict = segment_dict

        # for segment_search
        self.sel_query = None
        self.subnet_links_df = None
        self.graph = None

        # segment members
        self.segment_route_nodes = []

        if self.selection_dict:
            self.O_pk, self.D_pk = self._calculate_od_node_pks(self.selection_dict)

    def _calculate_od_node_pks(self) -> tuple:
        """Returns the primary key id for the AB nodes as a tuple.

        Returns: tuple of (origin node pk, destination node pk)
        """
        if set(["A", "B"]).issubset(self.selection_dict):
            _o_dict = self.selection_dict["A"]
            _d_dict = self.selection_dict["B"]
        elif set(["O", "D"]).issubset(self.segment_def_dict):
            _o_dict = self.selection_dict["O"]
            _d_dict = self.selection_dict["D"]
        else:
            raise SegmentFormatError()

        if len(_o_dict) > 1 or len(_d_dict) > 1:
            WranglerLogger.debug(f"_o_dict: {_o_dict}\n_d_dict: {_d_dict}")
            raise SegmentFormatError(
                "O and D of selection should have only one value each."
            )

        o_node_prop, o_val = next(iter(_o_dict.items()))
        d_node_prop, d_val = next(iter(_d_dict.items()))

        if o_node_prop != self.net.UNIQUE_NODE_KEY:
            _o_pk_list = self.net.nodes_df[
                self.net.nodes_df[o_node_prop] == o_val
            ].index.tolist()
            if len(_o_pk_list) != 1:
                WranglerLogger.error(
                    f"Node selectio for segment invalid. Found {len(_o_pk_list)} \
                    in nodes_df with {o_node_prop} = {o_val}. Should only find one!"
                )
            o_pk = _o_pk_list[0]
        else:
            o_pk = o_val

        if d_node_prop != self.net.UNIQUE_NODE_KEY:
            _d_pk_list = self.net.nodes_df[
                self.net.nodes_df[o_node_prop] == o_val
            ].index.tolist()
            if len(_d_pk_list) != 1:
                WranglerLogger.error(
                    f"Node selection for segment invalid. Found {len(_d_pk_list)} \
                    in nodes_df with {d_node_prop} = {d_val}. Should only find one!"
                )
            d_pk = _d_pk_list[0]
        else:
            d_pk = d_val

        return (o_pk, d_pk)

    @property
    def subnet_nodes(self):
        return self.net.nodes_in_links(self.subnet_links_df)

    @property
    def subnet_nodes_df(self):
        return self.net.nodes_df.loc[self.subnet_nodes]

    @property
    def segment_links_df(self):
        return self.net.links_df[
            self.net.links_df[self.net.LINK_FOREIGN_KEY_TO_NODE[0]].isin(
                self.segment_route_nodes
            )
            & self.net.links_df[self.net.LINK_FOREIGN_KEY_TO_NODE[1]].isin(
                self.segment_route_nodes
            )
        ]

    @property
    def segment_links(self):
        return self.segment_links_df.index.tolist()

    @property
    def segment_nodes_df(self):
        return self.net.nodes_df[self.net.nodes_df.loc(self.segment_route_nodes)]

    def connected_path_search(
        self,
        sp_weight_col: str = SP_WEIGHT_COL,
        sp_weight_factor: float = SP_WEIGHT_FACTOR,
        max_i=MAX_SEARCH_BREADTH,
    ) -> None:
        """
        Add links to the graph until
        (i) the A and B nodes are in the foreign key list
        - OR -
        (ii) reach maximum search breadth

        Args:
            sp_weight_col: column to use for weight of shortest path.
                Defaults to SP_WEIGHT_COL "i" (iteration)
            sp_weight_factor: optional weight to multiply the weight column by when finding
                the shortest path. Defaults to SP_WEIGHT_FACTOR, 100.
        """

        i = 0

        WranglerLogger.debug(f"Initial set of nodes: { self.subnet_nodes}".format())

        # expand network to find at least the origin and destination nodes
        while not {self.O_pk, self.D_pk}.issubset(self.subnet_nodes) and i <= max_i:
            i += 1
            WranglerLogger.debug(
                f"Adding breadth to find OD nodes in subnet - i/max_i: {i}/{max_i}"
            )
            self._expand_subnet_breadth(i=i)

        #  Once have A and B in graph try calculating shortest path
        WranglerLogger.debug("Calculating shortest path from graph")
        while (
            not self._find_subnet_shortest_path(sp_weight_col, sp_weight_factor)
            and i <= max_i
        ):
            i += 1
            WranglerLogger.debug(
                f"Adding breadth to find a connected path in subnet \
                i/max_i: {i}/{max_i}"
            )
            self._expand_subnet_breadth(i=i)

        if not self.found:
            WranglerLogger.debug(
                f"No connected path found from {self.O.pk} and {self.D_pk}\n\
                self.subnet_links_df:\n{self.subnet_links_df}"
            )
            raise SegmentSelectionError(
                f"No connected path found from {self.O.pk} and {self.D_pk}"
            )

    def _expand_subnet_breadth(
        self,
        i: int = None,
    ) -> None:
        """
        Add one degree of breadth to self.subnet_links_df and add property i =

        Args:
            i : iteration of adding breadth
        """
        WranglerLogger.debug("-Adding Breadth to Subnet-")

        if not i:
            WranglerLogger.warning("i not specified in _add_breadth, using 1")
            i = 1

        WranglerLogger.debug(f"Subnet Nodes: {self.subnet_nodes}")

        _outbound_links_df = copy.deepcopy(
            self.net.links_df.loc[
                self.net.links_df["A"].isin(self.subnet_nodes)
                & ~self.net.links_df["B"].isin(self.subnet_nodes)
            ]
        )
        _outbound_links_df["i"] = i
        _inbound_links_df = copy.deepcopy(
            self.net.links_df.loc[
                self.net.links_df["B"].isin(self.subnet_nodes)
                & ~self.net.links_df["A"].isin(self.subnet_nodes)
            ]
        )
        _inbound_links_df["i"] = i

        self.subnet_links_df = pd.concat(
            [self.subnet_links, _inbound_links_df, _outbound_links_df]
        )

        WranglerLogger.debug(
            f"Adding {len(_outbound_links_df)+len(_outbound_links_df)} links."
        )

    def _find_subnet_shortest_path(
        self,
        sp_weight_col: str = SP_WEIGHT_COL,
        sp_weight_factor: float = SP_WEIGHT_FACTOR,
    ) -> bool:
        """Creates osmnx graph of subnet links/nodes and tries to find a shortest path.

        Args:
            sp_weight_col (str, optional): _description_. Defaults to SP_WEIGHT_COL.
            sp_weight_factor (float, optional): _description_. Defaults to SP_WEIGHT_FACTOR.

        Returns:
            _type_: boolean indicating if shortest path was found
        """

        WranglerLogger.debug(
            f"Calculating shortest path from {self.O_pk} to {self.D_pk} using {sp_weight_col} as \
                weight with a factor of {sp_weight_factor}"
        )
        # Create Graph
        G = links_nodes_to_ox_graph(
            self.subnet_links_df,
            self.subnet_nodes_df,
            link_foreign_key_to_node=self.net.LINK_FOREIGN_KEY_TO_NODE,
            sp_weight_col=sp_weight_col,
            sp_weight_factor=sp_weight_factor,
        )

        self.segment_route_nodes = shortest_path(G, self.O_pk, self.D_pk, sp_weight_col)

        if not self.segment_route_nodes:
            WranglerLogger.debug(f"No SP from {self.O_pk} to {self.D_pk} Found.")
            return False

        return True


def identify_segment_endpoints(
    net,
    mode: str,
    min_connecting_links: int = 10,
    min_distance: float = None,
    max_link_deviation: int = 2,
):
    """

    Args:
        mode:  list of modes of the network, one of `drive`,`transit`,
            `walk`, `bike`
        links_df: if specified, will assess connectivity of this
            links list rather than self.links_df
        nodes_df: if specified, will assess connectivity of this
            nodes list rather than self.nodes_df

    """
    SEGMENT_IDENTIFIERS = ["name", "ref"]

    NAME_PER_NODE = 4
    REF_PER_NODE = 2

    _links_df, _nodes_df = filter_links_nodes_by_mode(
        net.links_df, net.nodes_df, modes=[mode]
    )

    _nodes_df = RoadwayNetwork.add_incident_link_data_to_nodes(
        links_df=_links_df,
        nodes_df=_nodes_df,
        link_variables=SEGMENT_IDENTIFIERS + ["distance"],
    )
    WranglerLogger.debug("Node/Link table elements: {}".format(len(_nodes_df)))

    # Screen out segments that have blank name AND refs
    _nodes_df = _nodes_df.replace(r"^\s*$", np.nan, regex=True).dropna(
        subset=["name", "ref"]
    )

    WranglerLogger.debug(
        "Node/Link table elements after dropping empty name AND ref : {}".format(
            len(_nodes_df)
        )
    )

    # Screen out segments that aren't likely to be long enough
    # Minus 1 in case ref or name is missing on an intermediate link
    _min_ref_in_table = REF_PER_NODE * (min_connecting_links - max_link_deviation)
    _min_name_in_table = NAME_PER_NODE * (min_connecting_links - max_link_deviation)

    _nodes_df["ref_freq"] = _nodes_df["ref"].map(_nodes_df["ref"].value_counts())
    _nodes_df["name_freq"] = _nodes_df["name"].map(_nodes_df["name"].value_counts())

    _nodes_df = _nodes_df.loc[
        (_nodes_df["ref_freq"] >= _min_ref_in_table)
        & (_nodes_df["name_freq"] >= _min_name_in_table)
    ]

    WranglerLogger.debug(
        "Node/Link table has n = {} after screening segments for min length:\n{}".format(
            len(_nodes_df),
            _nodes_df[
                [
                    RoadwayNetwork.UNIQUE_NODE_KEY,
                    "name",
                    "ref",
                    "distance",
                    "ref_freq",
                    "name_freq",
                ]
            ],
        )
    )
    # ----------------------------------------
    # Find nodes that are likely endpoints
    # ----------------------------------------

    # - Likely have one incident link and one outgoing link
    _max_ref_endpoints = REF_PER_NODE / 2
    _max_name_endpoints = NAME_PER_NODE / 2
    # - Attach frequency  of node/ref
    _nodes_df = _nodes_df.merge(
        _nodes_df.groupby(by=[RoadwayNetwork.UNIQUE_NODE_KEY, "ref"])
        .size()
        .rename("ref_N_freq"),
        on=[RoadwayNetwork.UNIQUE_NODE_KEY, "ref"],
    )
    # WranglerLogger.debug("_ref_count+_nodes:\n{}".format(_nodes_df[["model_node_id","ref","name","ref_N_freq"]]))
    # - Attach frequency  of node/name
    _nodes_df = _nodes_df.merge(
        _nodes_df.groupby(by=[RoadwayNetwork.UNIQUE_NODE_KEY, "name"])
        .size()
        .rename("name_N_freq"),
        on=[RoadwayNetwork.UNIQUE_NODE_KEY, "name"],
    )
    # WranglerLogger.debug("_name_count+_nodes:\n{}".format(_nodes_df[["model_node_id","ref","name","name_N_freq"]]))

    WranglerLogger.debug(
        "Possible segment endpoints:\n{}".format(
            _nodes_df[
                [
                    RoadwayNetwork.UNIQUE_NODE_KEY,
                    "name",
                    "ref",
                    "distance",
                    "ref_N_freq",
                    "name_N_freq",
                ]
            ]
        )
    )
    # - Filter possible endpoint list based on node/name node/ref frequency
    _nodes_df = _nodes_df.loc[
        (_nodes_df["ref_N_freq"] <= _max_ref_endpoints)
        | (_nodes_df["name_N_freq"] <= _max_name_endpoints)
    ]
    WranglerLogger.debug(
        "{} Likely segment endpoints with req_ref<= {} or freq_name<={} \n{}".format(
            len(_nodes_df),
            _max_ref_endpoints,
            _max_name_endpoints,
            _nodes_df[
                [
                    RoadwayNetwork.UNIQUE_NODE_KEY,
                    "name",
                    "ref",
                    "ref_N_freq",
                    "name_N_freq",
                ]
            ],
        )
    )
    # ----------------------------------------
    # Assign a segment id
    # ----------------------------------------
    _nodes_df["segment_id"], _segments = pd.factorize(_nodes_df.name + _nodes_df.ref)
    WranglerLogger.debug("{} Segments:\n{}".format(len(_segments), _segments))

    # ----------------------------------------
    # Drop segments without at least two nodes
    # ----------------------------------------

    # https://stackoverflow.com/questions/13446480/python-pandas-remove-entries-based-on-the-number-of-occurrences
    _nodes_df = _nodes_df[
        _nodes_df.groupby(["segment_id", RoadwayNetwork.UNIQUE_NODE_KEY])[
            RoadwayNetwork.UNIQUE_NODE_KEY
        ].transform(len)
        > 1
    ]

    WranglerLogger.debug(
        "{} Segments with at least nodes:\n{}".format(
            len(_nodes_df),
            _nodes_df[[RoadwayNetwork.UNIQUE_NODE_KEY, "name", "ref", "segment_id"]],
        )
    )

    # ----------------------------------------
    # For segments with more than two nodes, find farthest apart pairs
    # ----------------------------------------

    def _max_segment_distance(row):
        _segment_nodes = _nodes_df.loc[_nodes_df["segment_id"] == row["segment_id"]]
        dist = _segment_nodes.geometry.distance(row.geometry)
        return max(dist.dropna())

    _nodes_df["seg_distance"] = _nodes_df.apply(_max_segment_distance, axis=1)
    _nodes_df = _nodes_df.merge(
        _nodes_df.groupby("segment_id")
        .seg_distance.agg(max)
        .rename("max_seg_distance"),
        on="segment_id",
    )

    _nodes_df = _nodes_df.loc[
        (_nodes_df["max_seg_distance"] == _nodes_df["seg_distance"])
        & (_nodes_df["seg_distance"] > 0)
    ].drop_duplicates(subset=[RoadwayNetwork.UNIQUE_NODE_KEY, "segment_id"])

    # ----------------------------------------
    # Reassign segment id for final segments
    # ----------------------------------------
    _nodes_df["segment_id"], _segments = pd.factorize(_nodes_df.name + _nodes_df.ref)

    WranglerLogger.debug(
        "{} Segments:\n{}".format(
            len(_segments),
            _nodes_df[
                [
                    RoadwayNetwork.UNIQUE_NODE_KEY,
                    "name",
                    "ref",
                    "segment_id",
                    "seg_distance",
                ]
            ],
        )
    )

    return _nodes_df[
        ["segment_id", RoadwayNetwork.UNIQUE_NODE_KEY, "geometry", "name", "ref"]
    ]
