import copy

import pandas as pd
from geopandas import GeoDataFrame

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
