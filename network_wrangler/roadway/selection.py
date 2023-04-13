import hashlib

from typing import Any, Collection, Mapping

import pandas as pd
from geopandas import GeoDataFrame

from ..logger import WranglerLogger

from .segment import Segment
from .graph import shortest_path,links_nodes_to_ox_graph,SP_WEIGHT_COL,SP_WEIGHT_FACTOR

class SelectionFormatError(Exception):
    pass

class SelectionError(Exception):
    pass

MODES_TO_NETWORK_LINK_VARIABLES = {
    "drive": ["drive_access"],
    "bus": ["bus_only", "drive_access"],
    "rail": ["rail_only"],
    "transit": ["bus_only", "rail_only", "drive_access"],
    "walk": ["walk_access"],
    "bike": ["bike_access"],
}

MODES_TO_NETWORK_NODE_VARIABLES = {
    "drive": ["drive_node"],
    "rail": ["rail_only", "drive_node"],
    "bus": ["bus_only", "drive_node"],
    "transit": ["bus_only", "rail_only", "drive_node"],
    "walk": ["walk_node"],
    "bike": ["bike_node"],
}
class RoadwaySelection():
    """_summary_

    Properties:
        net:
        id:
        type: one of "unique_link_id", "unique_node_id" or "segment_search"

    """

    def __init__(self,net:'RoadwayNetwork',selection_dict:dict,additional_requirements = {"drive_access": True},ignore = []):
        self.net = net

        # This should make it compatible with old and new project card types
        self.selection_dict = selection_dict.update(selection_dict.get("links",{}))
        self.sel_key = RoadwaySelection._assign_selection_key(self.selection_dict)
        self.select = None
        self.type = self._assign_selection_type(self.selection_dict)
        self.additional_requirements = additional_requirements
        self.ignore = ignore + ["O","D","A","B"]

        self.selected_links_df = None

        if self.additional_requirements:
            for k,v in additional_requirements.items():
                self.selection_dict[k]=v

        self.segment = None

        if self.type == "segment_search":
            self.segment = Segment(self.net, self.selection_dict)
            
    @property
    def selected_links(self) -> list:
        if not self.found:
            return None
        return self.selected_links_df.index.tolist()
    
    @property
    def found(self) ->bool:
        if self.selected_links_df is not None:
            return True
        return False
    
    @staticmethod
    def _assign_selection_key(selection_dict: dict) -> tuple:
        """
        Selections are stored by a hash of the selection dictionary.

        Args:
            selection_dictonary: Selection Dictionary

        Returns: Hex code for hash
        """

        return hashlib.md5(b"selection_dict").hexdigest()

    def _assign_selection_type(self,selection_dict: dict):
        selection_keys = list(selection_dict.keys()) 
        
        # if selection has a unique id, then its a unique_id type
        if not set(self.net.UNIQUE_MODEL_LINK_IDENTIFIERS).isdisjoint(selection_keys):
            self.select = self._select_unique_link_id
            return "unique_link_id"
        
        elif set(["A","B","name"]).issubset(selection_keys):
            self.select = self._select_roadway_segment
            return "segment_search"
        
        elif set(["O","D","name"]).issubset(selection_keys):
            self.select = self._select_roadway_segment
            return "segment_search"
        
        else:
            WranglerLogger.error(f"Selection type not found for : {selection_dict}")
            WranglerLogger.error(f"Expected one of: {self.net.UNIQUE_MODEL_LINK_IDENTIFIERS} or A, B, name or O, D, name")
            raise SelectionFormatError("Don't believe selection is valid - can't find a unique id or A, B, Name or O,D name")

    def _validate_link_selection(self, selection: dict) -> bool:
        """Validates that link selection is complete/valid for given network.

        Checks:
        1. selection properties for links, a, and b are in links_df
        2. either a unique ID or name + A & B are specified

        If selection for links is "all" it is assumed valid.

        Args:
            selection (dict): selection dictionary

        Returns:
            bool: True if link selection is valid and complete.
        """

        if selection.get("links") == "all":
            return True

        valid = True

        _link_selection_props = [p for x in selection["links"] for p in x.keys()]

        _missing_link_props = set(_link_selection_props) - set(self.links_df.columns)

        if _missing_link_props:
            WranglerLogger.error(
                f"Link selection contains properties not found in the link dataframe:\n\
                {','.join(_missing_link_props)}"
            )
            valid = False

        _link_explicit_link_id = bool(
            set(RoadwayNetwork.EXPLICIT_LINK_IDENTIFIERS).intersection(
                set(_link_selection_props)
            )
        )
        # if don't have an explicit link id, then require A and B nodes
        _has_alternate_link_id = all(
            [
                selection.get("A"),
                selection.get("B"),
                any([x.get("name") for x in selection["links"]]),
            ]
        )

        if not _link_explicit_link_id and not _has_alternate_link_id:
            WranglerLogger.error(
                "Link selection does not contain unique link ID or alternate A and B nodes + 'name'."
            )
            valid = False

        _node_selection_props = list(
            set(
                list(selection.get("A", {}).keys())
                + list(selection.get("B", {}).keys())
            )
        )
        _missing_node_props = set(_node_selection_props) - set(self.nodes_df.columns)

        if _missing_node_props:
            WranglerLogger.error(
                f"Node selection contains properties not found in the node dataframe:\n\
                {','.join(_missing_node_props)}"
            )
            valid = False

        if not valid:
            raise ValueError("Link Selection is not valid for network.")
        return True

    def _validate_node_selection(self, selection: dict) -> bool:
        """Validates that node selection is complete/valid for given network.

        Checks:
        1. selection properties for nodes are in nodes_df
        2. Nodes identified by an explicit or implicit unique ID. A warning is given for using
            a property as an identifier which isn't explicitly unique.

        If selection for nodes is "all" it is assumed valid.

        Args:
            selection (dict): Project Card selection dictionary

        Returns:
            bool:True if node selection is valid and complete.
        """
        valid = True

        if selection.get("nodse") == "all":
            return True

        _node_selection_props = [p for x in selection["nodes"] for p in x.keys()]

        _missing_node_props = set(_node_selection_props) - set(self.nodes_df.columns)

        if _missing_node_props:
            WranglerLogger.error(
                f"Node selection contains properties not found in the node dataframe:\n\
                {','.join(_missing_node_props)}"
            )
            valid = False

        _has_explicit_unique_node_id = bool(
            set(self.net.UNIQUE_NODE_IDENTIFIERS).intersection(
                set(_node_selection_props)
            )
        )

        if not _has_explicit_unique_node_id:
            if self.net.nodes_df[_node_selection_props].get_value_counts().min() == 1:
                WranglerLogger.warning(
                    f"Link selection does not contain an explicit unique link ID: \
                        {self.net.UNIQUE_NODE_IDENTIFIERS}, \
                        but has properties which sufficiently select a single node. \
                        This selection may not work on other networks."
                )
            else:
                WranglerLogger.error(
                    "Link selection does not contain unique link ID or alternate A and B nodes + 'name'."
                )
                valid = False
        if not valid:
            raise ValueError("Node Selection is not valid for network.")
        return True

    def validate_selection(self, selection: dict) -> bool:
        """
        Evaluate whetther the selection dictionary contains the
        minimum required values.

        Args:
            selection: selection dictionary to be evaluated

        Returns: boolean value as to whether the selection dictonary is valid.
        """
        if selection.get("links"):
            return self._validate_link_selection(selection)

        elif selection.get("nodes"):
            return self._validate_node_selection(selection)

        else:
            raise ValueError(
                f"Project Card Selection requires either 'links' or 'nodes' : \
                Selection provided: {selection.keys()}"
            )

    def _select_unique_link_id(
        self,
    ):
        """_summary_

         Args:
            selection_dict: selection dictionary 
        """

        _sel_links_mask = self.links_df.isin(self.selection_dict).any(axis=1)
        self.selected_links_df = self.net.links_df.loc[_sel_links_mask]

        if not _sel_links_mask.any():
            WranglerLogger.warning("No links found matching criteria.")
            return False
        
        return True
    
    def _dict_to_query(
        selection_dict: Mapping[str, Any],
    )-> str: 
        """Generates the initial query of candidate links for a roadway segment.

        Args:
            selection_dict: selection dictionary 

        Returns:
            _type_: Query value
        """
        WranglerLogger.debug("Building selection query")
        def _kv_to_query_part(k,v,_q_part=''):
            if isinstance(v,list):
                _q_part += "(" + " or ".join([_kv_to_query_part(k,i) for i in v]) + ")"
                return _q_part
            if isinstance(v, str):
                return k + '.str.contains("' + v + '")'
            else:
                return k + "==" + str(v)
            
        query = "(" + " and ".join([_kv_to_query_part(k,v) for k,v in selection_dict.items()]) + ")"
        WranglerLogger.debug(f"Selection query:\n{query}")
        return query
    
    def _select_roadway_segment(
        self,
    ):
        """_summary_

        """
        # identify candidate links which match the initial query
        # assign them as iteration = 0
        # subsequent iterations that didn't match the query will be
        # assigned a heigher weight in the shortest path
        
        WranglerLogger.debug("Selecting segment of connected links")
        _selection_dict = self.selection_dict.copy()

        # First search for initial set of links using "name" and if that doesn't return any links, 
        #   search using "ref"
        if "ref" in _selection_dict:
            _selection_dict["name"] += _selection_dict["ref"]
            del _selection_dict["ref"]

        self.sel_query = self._segment_query(_selection_dict)
        self.subnet_links_df = self.links_df.query(self.sel_query, engine="python")
        if len(self.subnet_links_df) == 0:
            WranglerLogger.warning(f"No links found using selection of name =  {_selection_dict['name']}.") 
            if "ref" not in self.selection_dict:
                raise SegmentSelectionError("No links found with selection.")
            else:
                del _selection_dict["name"]
                _selection_dict["ref"] = self.selection_dict["ref"]

                WranglerLogger.info(f"Trying ref = {self.selection_dict['ref']} instead")
                self.sel_query = self._segment_query(_selection_dict)
                self.subnet_links_df = self.links_df.query(self.sel_query, engine="python")

                if len(self.subnet_links_df) == 0:
                    WranglerLogger.error(f"No links found using selection of ref =  {_selection_dict['ref']}.") 
                    raise SegmentSelectionError("No links found with selection.")
            
        # i is iteration # for an iterative search for connected paths with progressively larger subnet
        self.subnet_links_df["i"] = 0

        self.path_search(
            self.selections[sel_key]["candidate_links"],
            A_pk,
            B_pk,
            weight_factor=RoadwayNetwork.SP_WEIGHT_FACTOR,
        )

        # Conduct a "selection on the selection" if have additional requirements to satisfy
        else:
            resel_query = ProjectCard.build_selection_query(
                selection=selection,
                unique_ids=RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS,
                mode=RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES[search_mode],
                ignore=["name"],
            )
            WranglerLogger.debug("Reselecting features:\n{}".format(resel_query))
            self.selections[sel_key]["selected_links"] = self.selections[sel_key][
                "links"
            ].query(resel_query, engine="python")

        if len(self.selections[sel_key]["selected_links"]) > 0:
            self.selections[sel_key]["selection_found"] = True
        else:
            raise SelectionError(f"No links found for selection: {selection}")

        self.selections[sel_key]["selection_found"] = True
        return self.selections[sel_key]["selected_links"].index.tolist()


def filter_links_nodes_by_mode(
    links_df: pd.DataFrame, nodes_df: pd.DataFrame, modes: list[str] = None
) -> tuple(pd.DataFrame, pd.DataFrame):
    """Returns nodes and link dataframes for specific mode.

    Args:
        links_df: DataFrame of standard network links
        nodes_df: DataFrame of standard network nodes
        modes: list of the modes of the network to be kept, must be in
            `drive`,`transit`,`rail`,`bus`,`walk`, `bike`.
            For example, if bike and walk are selected, both bike and walk links will be kept.

    Returns: tuple of DataFrames for links, nodes filtered by mode

    .. todo:: Right now we don't filter the nodes because transit-only
    links with walk access are not marked as having walk access
    Issue discussed in https://github.com/wsp-sag/network_wrangler/issues/145
    modal_nodes_df = nodes_df[nodes_df[mode_node_variable] == 1]
    """

    if not set(modes).issubset(list(MODES_TO_NETWORK_LINK_VARIABLES.keys())):
        raise SelectionFormatError(f"Modes: {modes} not all in network: {MODES_TO_NETWORK_LINK_VARIABLES.keys()}")
  
    if not set(modes).issubset(list(MODES_TO_NETWORK_NODE_VARIABLES.keys())):
        raise SelectionFormatError(f"Modes: {modes} not all in network: {MODES_TO_NETWORK_LINK_VARIABLES.keys()}")
  

    _mode_link_props = list(
        set(
            [
                m
                for m in modes
                for m in MODES_TO_NETWORK_LINK_VARIABLES[m]
            ]
        )
    )
    _mode_node_props = list(
        set(
            [
                m
                for m in modes
                for m in MODES_TO_NETWORK_NODE_VARIABLES[m]
            ]
        )
    )

    modal_links_df = links_df.loc[links_df[_mode_link_props].any(axis=1)]

    # TODO right now we don't filter the nodes because transit-only
    # links with walk access are not marked as having walk access
    # Issue discussed in https://github.com/wsp-sag/network_wrangler/issues/145
    # modal_nodes_df = nodes_df[nodes_df[mode_node_variable] == 1]
    modal_nodes_df = nodes_df

    return modal_links_df, modal_nodes_df