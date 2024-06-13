"""Subnet class for RoadwayNetwork object."""

from __future__ import annotations
import copy
import hashlib
from typing import TYPE_CHECKING

import pandas as pd

from pandera.typing import DataFrame

from .graph import links_nodes_to_ox_graph
from .links.links import node_ids_in_links
from ..logger import WranglerLogger

if TYPE_CHECKING:
    from .network import RoadwayNetwork
    from ..models.roadway.tables import RoadLinksTable, RoadNodesTable
    from networkx import MultiDiGraph


class SubnetExpansionError(Exception):
    """Raised when a subnet can't be expanded to include a node or set of nodes."""

    pass


class SubnetCreationError(Exception):
    """Raised when a subnet can't be created."""

    pass


class Subnet:
    """Subnet is a connected selection of links/nodes from a RoadwayNetwork object.

    Subnets are used for things like identifying Segments.

    Usage:

    ```
    selection_dict = {
        "links": {"name":['6th','Sixth','sixth']},
        "from": {"osm_node_id": '187899923'},
        "to": {"osm_node_id": '187865924'}
    }

    segment = Segment(net = RoadwayNetwork(...), selection_dict = selection_dict)
    # used to store graph
    self._segment_route_nodes = shortest_path(segment.subnet.graph,start_node_pk,end_node_pk)
    ```

    attr:
        net: Associated RoadwayNetwork object
        selection_dict: segment selection dictionary, which is is used to create initial subnet
            based on name and ref
        subnet_links_df: initial subnets can alternately be defined by a dataframe of links.
        graph_hash: unique hash of subnet_links_df, _sp_weight_col and _sp_weight_factor. Used
            to identify if any of these have changed and thus if a new graph should be generated.
        graph: returns the nx.MultiDigraph of subne which is stored in self._graph and lazily
            evaluated when called if graph_hash has changed becusae it is an expensive operation.
        num_links: number of links in the subnet
        subnet_nodes: lazily evaluated list of node primary keys based on subnet_links_df
        subnet_nodes_df: lazily evaluated selection of net.nodes_df based on subnet_links_df

    """

    def __init__(
        self,
        net: RoadwayNetwork,
        modes: list = ["drive"],
        subnet_links_df: pd.DataFrame = None,
        selection_dict: dict = None,
        i: int = 0,
        sp_weight_col: str = "weight",
        sp_weight_factor=1,
        max_search_breadth=10,
    ):
        """Generates and returns a Subnet object.

        Args:
            net (RoadwayNetwork): Associated RoadwayNetwork object.
            modes: List of modes to limit subnet to. Defaults to "drive".
            subnet_links_df (pd.DataFrame, optional): Initial links to include in subnet.
                Optional if define a selection_dict and will default to result of
                self.generate_subnet_from_selection_dict(selection_dict)
            selection_dict (dict optional): RoadwayLinkSelection dictionary for initial links to
                include in subnet.
            i: Expansion iteration number. Shouldn't need to change this as it will be done
                internally. Defaults to 0.
            sp_weight_col: Column to use for weights in shortest path.  Will not
                likely need to be changed. Defaults to "weight".
            sp_weight_factor: Factor to multiply sp_weight_col by to use for
                weights in shortest path.  Will not likely need to be changed. Defaults to 1.
            max_search_breadth: Maximum expansions of the subnet network to find
                the shortest path after the initial selection based on `name`. Will not likely
                need to be changed unless network contains a lot of ambiguity. Defaults to 10.

        Example:
                    ```
                    selection_dict = {
                        "name":['6th','Sixth','sixth'],
                        "from": {"osm_node_id": '187899923'},
                        "to": {"osm_node_id": '187865924'}
                    }
                    ```
                Defaults to None.
            i (int, optional): Expansion iteration number. Shouldn't need to change this.
                Defaults to 0.
            sp_weight_col (str, optional): Column to use for weights in shortest path.  Will not
                likely need to be changed. Defaults to "weight"".
            sp_weight_factor (int, optional): Factor to multiply sp_weight_col by to use for
                weights in shortest path.  Will not likely need to be changed. Defaults to 1`.
            max_search_breadth (int, optional):Maximum expansions of the subnet network to find
                the shortest path after the initial selection based on `name`. Will not likely
                need to be changed unless network contains a lot of ambiguity. Defaults to 10.

        Raises:
            ValueError: _description_
        """
        self.net = net
        self.modes = modes
        self._subnet_links_df = (
            None if subnet_links_df is None else subnet_links_df.mode_query(self.modes)
        )
        self.selection_dict = selection_dict

        self._i = i
        self._sp_weight_col = sp_weight_col
        self._sp_weight_factor = sp_weight_factor
        self._max_search_breadth = max_search_breadth
        self._graph = None
        self._graph_link_hash = None

    @property
    def exists(self) -> bool:
        """Returns True if subnet_links_df is not None and has at least one link."""
        if self.subnet_links_df is None:
            return False
        if len(self.subnet_links_df) == 0:
            return False
        if len(self.subnet_links_df) > 0:
            return True
        raise SubnetCreationError("Something's not right.")

    @property
    def subnet_links_df(self) -> DataFrame[RoadLinksTable]:
        """Links in the subnet. If not set, will generate from selection_dict."""
        if self._subnet_links_df is None:
            if self.selection_dict is None:
                raise SubnetCreationError(
                    "Cannot create a subnet without one of subnet_links_df\
                    or selection_dict"
                )

            selection = self.net.get_selection(self.selection_dict)
            subnet_links_df = copy.deepcopy(selection.selected_links_df)
            subnet_links_df["i"] = 0
            self._subnet_links_df = subnet_links_df
        return self._subnet_links_df

    @property
    def graph_hash(self) -> str:
        """Hash of the links in order to detect a network change from when graph created."""
        _value = [
            self.subnet_links_df.df_hash(),
            self._sp_weight_col,
            str(self._sp_weight_factor),
        ]
        _enc_value = str.encode("-".join(_value))
        _hash = hashlib.sha256(_enc_value).hexdigest()
        return _hash

    @property
    def graph(self) -> MultiDiGraph:
        """nx.MultiDiGraph of the subnet."""
        if self.graph_hash != self._graph_link_hash:
            self._graph = links_nodes_to_ox_graph(
                self.subnet_links_df,
                self.subnet_nodes_df,
                sp_weight_col=self._sp_weight_col,
                sp_weight_factor=self._sp_weight_factor,
            )
        return self._graph

    @property
    def num_links(self):
        """Number of links in the subnet."""
        return len(self.subnet_links_df)

    @property
    def subnet_nodes(self) -> list[int]:
        """List of node_ids in the subnet."""
        if self.subnet_links_df is None:
            raise ValueError("Must set self.subnet_links_df before accessing subnet_nodes.")
        return node_ids_in_links(self.subnet_links_df, self.net.nodes_df)

    @property
    def subnet_nodes_df(self) -> DataFrame[RoadNodesTable]:
        """Nodes filtered to subnet."""
        return self.net.nodes_df.loc[self.subnet_nodes]

    def expand_subnet_to_include_nodes(self, nodes_list: list):
        """Expand network to include list of nodes.

        Will stop expanding and generate a SubnetExpansionError if meet max_search_breadth before
        finding the nodes.

        Args:
            nodes_list: a list of node primary keys to expand subnet to include.
        """
        WranglerLogger.debug(f"Expanding subnet to includes nodes: {nodes_list}")

        # expand network to find nodes in the list
        while (
            not set(nodes_list).issubset(self.subnet_nodes) and self._i <= self._max_search_breadth
        ):
            self._expand_subnet_breadth()

        if not set(nodes_list).issubset(self.subnet_nodes):
            raise SubnetExpansionError(
                f"Can't find nodes {nodes_list} before achieving maximum\
                network expansion iterations of {self._max_search_breadth}"
            )

    def _expand_subnet_breadth(self) -> None:
        """Add one degree of breadth to self.subnet_links_df and add property."""
        self._i += 1

        WranglerLogger.debug(
            f"Adding Breadth to Subnet: \
            i={self._i} out of {self._max_search_breadth}"
        )
        _modal_links_df = self.net.links_df.mode_query(self.modes)
        # find links where A node is connected to subnet but not B node
        _outbound_links_df = _modal_links_df.loc[
            _modal_links_df[self.net.links_df.params.from_node].isin(self.subnet_nodes)
            & ~_modal_links_df[self.net.links_df.params.to_node].isin(self.subnet_nodes)
        ]

        WranglerLogger.debug(f"_outbound_links_df links: {len(_outbound_links_df)}")

        # find links where B node is connected to subnet but not A node
        _inbound_links_df = _modal_links_df.loc[
            _modal_links_df[self.net.links_df.params.to_node].isin(self.subnet_nodes)
            & ~_modal_links_df[self.net.links_df.params.from_node].isin(self.subnet_nodes)
        ]

        WranglerLogger.debug(f"_inbound_links_df links: {len(_inbound_links_df)}")

        # find links where A and B nodes are connected to subnet but not in subnet
        _both_AB_connected_links_df = _modal_links_df.loc[
            _modal_links_df[self.net.links_df.params.to_node].isin(self.subnet_nodes)
            & _modal_links_df[self.net.links_df.params.from_node].isin(self.subnet_nodes)
            & ~_modal_links_df.index.isin(self.subnet_links_df.index.tolist())
        ]

        WranglerLogger.debug(
            f"{len(_both_AB_connected_links_df)} links where both A and B are connected to subnet\
             but aren't in subnet."
        )

        _add_links_df = pd.concat(
            [_both_AB_connected_links_df, _inbound_links_df, _outbound_links_df]
        )

        _add_links_df["i"] = self._i
        WranglerLogger.debug(f"Links to add: {len(_add_links_df)}")

        WranglerLogger.debug(f"{self.num_links} initial subnet links")

        self._subnet_links_df = pd.concat([self.subnet_links_df, _add_links_df])

        WranglerLogger.debug(f"{self.num_links} expanded subnet links")
