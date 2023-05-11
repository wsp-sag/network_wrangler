import pandas as pd

import copy
import hashlib
from typing import Any, Mapping

from ..logger import WranglerLogger
from ..utils import delete_keys_from_dict, coerce_dict_to_df_types
from .segment import Segment


class SelectionFormatError(Exception):
    pass


class SelectionError(Exception):
    pass


# Project card keys that are associated with node properties
NODE_PROJECT_CARD_KEYS = ["nodes", "from", "to"]

# Project card keys that are associated with link properties
LINK_PROJECT_CARD_KEYS = [ "links"  ] 

# Default modes for searching in the event it is not specified in the project card using `modes` keyword.
DEFAULT_SEARCH_MODES = ["drive"]

class RoadwaySelection:
    """Object to perform and store information about a selection from a project card "facility".

    Properties:
        net: roadway network related to the selection
        sel_key: unique selection key based on selection dictionary
        selection_dictionary
        modes: list of modes that to search for links for. Provided using the "modes" keyword in
            the selection_dictionary. If not provided, will default to DEFAULT_SEARCH_MODES
            which defaults to ["drive"]
        link_selection_dict: all selection criteria for links
        node_selection_dict: all selection criteria for nodes
        explicit_link_id_selection_dict: link selection criteria that uses explicit IDs
        explicit_node_id_selection_dict: node selection criteria that uses explicit IDs
        additional_link_selection_dict: link selection criteria that is layered on top of a 
            segment-search, all, or explict ID search (i.e. "lanes": [1,2,3], "drive_access": True) 
        additional_node_selection_dict: node selection criteria that is layered on top of a 
            all or explicit ID search. Not typically used. 
        selection_type(str): one of "explicit_link_id", "explicit_node_id" or "segment_search" or "all_links" or
            "all_nodes"
        selected_nodes_df: lazily-evaluated selected node. sDefaults to None if not a
            nodes selection.
        selected_links_df: lazily-evaluated selected links. Defaults to None if not a
            links selection.
        link_selection(bool): True if selection is for links
        node_selection(bool): True if selection is for nodes
        feature_types(list): list of selected features containing  "links" and/or "nodes
    """

    def __init__(
        self,
        net: "RoadwayNetwork",
        selection_dict: dict,
    ):
        """Constructor for RoadwaySelection object.

        Args:
            net (RoadwayNetwork): Roadway network object to select from.
            selection_dict (dict): Selection dictionary. 
        """        
        self.net = net

        self.selection_dict = selection_dict

        if "modes" in selection_dict:
            self.modes = selection_dict["modes"]
        else:
            self.modes = DEFAULT_SEARCH_MODES

        self.link_selection_dict = self.calc_link_selection_dict(self.selection_dict)
        self.node_selection_dict = self.calc_node_selection_dict(self.selection_dict)

        WranglerLogger.debug(f"self.selection_dict:\n {self.selection_dict}")
        WranglerLogger.debug(f"self.modes:\n {self.modes}")
        WranglerLogger.debug(f"self.link_selection_dict:\n {self.link_selection_dict}")
        WranglerLogger.debug(f"self.node_selection_dict:\n {self.node_selection_dict}")

        self.sel_key = RoadwaySelection._assign_selection_key(self.selection_dict)

        self.selection_type = self.get_selection_type(self.selection_dict, self.net)
        WranglerLogger.debug(f"self.selection_type:\n {self.selection_type}")
        self._stored_net_hash = copy.deepcopy(self.net.network_hash)
        self._selected_links_df = None
        self._selected_nodes_df = None
        self._segment = None

        self.validate_selection()

    def __nonzero__(self):
        if len(self.selected_links)>0:
            return True
        if len(self.selected_nodes)>0:
            return True
        return False


    @property
    def selected_links(self) -> list:
        if self.selected_links_df is None:
            return []
        return self.selected_links_df.index.tolist()

    @property
    def selected_nodes(self) -> list:
        if self.selected_nodes_df is None:
            return []
        return self.selected_nodes_df.index.tolist()

    @property
    def found(self) -> bool:
        if self.selected_links_df is not None:
            return True
        if self.selected_nodes_df is not None:
            return True
        return False

    @property
    def segment(self):
        if self._segment is None and self.selection_type == "segment_search":
            WranglerLogger.debug("Creating new segment")
            self._segment = Segment(self.net, self)
        return self._segment

    @property
    def link_selection(self) -> bool:
        LINK_SELECTIONS = ["all_links", "explicit_link_id", "segment_search"]
        return self.selection_type in LINK_SELECTIONS

    @property
    def node_selection(self) -> bool:
        NODE_SELECTIONS = ["all_nodes", "explicit_node_id"]
        return self.selection_type in NODE_SELECTIONS

    @property
    def feature_types(self) -> list:
        _t = []
        if self.node_selection:
            _t.append("nodes")
        if self.link_selection:
            _t.append("links")
        if not _t:
            raise SelectionFormatError(
                "Should be one of type node or links but found neither."
            )
        return _t

    def calc_node_selection_dict(self,selection_dict) -> list:
        """Dictionary of properties nodes are selected by in following sub-dictionaries:
        
        Also coerces the values to the matching type of the network nodes.

        - "nodes": should be either "all" or a dictionary of properties.
        - "from": should be of length 1 and only be there for segment-based search
        - "to": should be of length 1 and only be there for segment-based search
        """
        _node_prop_dict = {k:v for k,v in selection_dict.items() if k in NODE_PROJECT_CARD_KEYS}
        _typed_node_prop_dict = {
            k:coerce_dict_to_df_types(v,self.net.nodes_df, skip_keys = ["all"]) for k,v in _node_prop_dict.items() 
        }
        return _typed_node_prop_dict

    def calc_link_selection_dict(self,selection_dict) -> list:
        """Dictionary of link selection properties are selected by.
        
        Also coerces the values to the matching type of the network links.
        """
        _link_prop_dict = {k:v for i in LINK_PROJECT_CARD_KEYS for k,v in selection_dict.get(i,{}).items()}
        _typed_link_prop_dict = coerce_dict_to_df_types(_link_prop_dict,self.net.links_df, skip_keys = ["all"])

        return _typed_link_prop_dict

    @property
    def explicit_link_id_selection_dict(self):
        _eid_sel_dict = {k:v for k,v in self.link_selection_dict.items() if k in self.net.links_df.params.explicit_ids}
        return _eid_sel_dict
    
    @property
    def explicit_node_id_selection_dict(self):
        _eid_sel_dict = {k:v for k,v in self.node_selection_dict.items() if k in self.net.nodes_df.params.explicit_ids}
        return _eid_sel_dict
    
    @property
    def additional_link_selection_dict(self):
        _exclude = self.net.links_df.params.explicit_ids+["name","link","links","ref"]
        _eid_sel_dict = {k:v for k,v in self.link_selection_dict.items() if k not in _exclude}
        return _eid_sel_dict
    
    @property
    def additional_node_selection_dict(self):
        _exclude = self.net.links_df.params.explicit_ids+["node","nodes"]
        _eid_sel_dict = {k:v for k,v in self.node_selection_dict.items() if k not in _exclude}
        return _eid_sel_dict

    @staticmethod
    def _assign_selection_key(selection_dict: dict) -> tuple:
        """
        Selections are stored by a sha1 hash of the bit-encoded string of the selection dictionary.

        Args:
            selection_dictonary: Selection Dictionary

        Returns: Hex code for hash
        """
        return hashlib.sha1(str(selection_dict).encode()).hexdigest()

    @staticmethod
    def get_selection_type(selection_dict: dict, net) -> str:
        """Determines what type of selection it is based on format of selection dict.

        Args:
            selection_dict (dict): Selection dictionary from project card.
            net: Roadway Network for selection

        Returns:
            str: Selection type value
        """
        SEGMENT_SELECTION = ["from","to","links"]
        NODE_SELECTION = ["nodes"]
        selection_keys = list(selection_dict.keys())

        if set(SEGMENT_SELECTION).issubset(selection_keys):
            return "segment_search"
        
        if "all" in selection_dict.get("links",{}):
            return "all_links"

        if "all" in selection_dict.get("nodes",{}):
            return "all_nodes"

        if not set(net.links_df.params.explicit_ids).isdisjoint(selection_dict.get("links",{})):
            return "explicit_link_id"

        if not set(net.nodes_df.params.unique_ids).isdisjoint(selection_dict.get("nodes",{})):
            return "explicit_node_id"

        WranglerLogger.error(f"Selection type not found for : {selection_dict}")
        WranglerLogger.error(
            f"Expected one of: {net.links_df.params.unique_ids} or A, B, name or O, D, name"
        )
        raise SelectionFormatError(
            "Don't believe selection is valid - can't find a unique id or A, B, \
                Name or O,D name"
        )

    @property
    def selected_links_df(self) -> pd.DataFrame:
        """Lazily evaluates selection for links or returns stored value in self._selected_links_df.

        Will re-evaluate if the current network hash is different than the stored one from the
        last selection.

        Returns:
            _type_: _description_
        """
        if not self.link_selection:
            msg = f"selected_links_df accessed for invalid selection type: {self.selection_type}"
            WranglerLogger.error(msg)
            raise SelectionError(msg)
        if (
            self._selected_links_df is not None
        ) and self._stored_net_hash == self.net.network_hash:
            return self._selected_links_df
        
        self._stored_net_hash = copy.deepcopy(self.net.network_hash)

        if self.selection_type == "explicit_link_id":
            WranglerLogger.info("Selecting links based on explict IDs ...")
            _selected_links_df = self._select_explicit_link_id()

        elif self.selection_type == "segment_search":
            WranglerLogger.debug("Selecting links from a segment...")
            _selected_links_df = self.segment.segment_links_df
            
        elif self.selection_type == "all_links":
            WranglerLogger.debug("Selecting all links...")
            _selected_links_df = self.net.links_df.mode_query(modes=self.modes)
        else:
            raise SelectionFormatError("Doesn't have known link selection type")
        
        if self.additional_link_selection_dict:
            WranglerLogger.debug(f"Initially selected links: {len(_selected_links_df)}")
            if len(_selected_links_df) < 10:
                _cols = _selected_links_df.__dict__["params"].display_cols
                WranglerLogger.debug(f"\n{_selected_links_df[_cols]}")
            
            WranglerLogger.debug(f"Selecting from selection based on: {self.additional_link_selection_dict}")
            _selected_links_df = _selected_links_df.dict_query(self.additional_link_selection_dict)
            
            WranglerLogger.info
            (f"Selection with additional restrictions links: {len(_selected_links_df)}")

        self._selected_links_df = _selected_links_df

        WranglerLogger.info(f"Final selected links: {len(_selected_links_df)}")
        if len(_selected_links_df) < 10:
            _cols = _selected_links_df.__dict__["params"].display_cols
            WranglerLogger.debug(f"\n{_selected_links_df[_cols]}")

        return self._selected_links_df

    @property
    def selected_nodes_df(self) -> pd.DataFrame:
        """Lazily evaluates selection for nodes or returns stored value in self._selected_nodes_df.

        Will re-evaluate if the current network hash is different than the stored one from the
        last selection.
        """
        if not self.node_selection:
            msg = f"selected_nodes_df accessed for invalid selection type: {self.selection_type}"
            WranglerLogger.error(msg)
            raise SelectionError(msg)
        if (
            self._selected_nodes_df is not None
        ) and self._stored_net_hash == self.net.network_hash:
            return self._selected_nodes_df

        self._stored_net_hash = copy(self.net.network_hash)

        if self.selection_type == "explicit_node_id":
            _init_selected_nodes_df = self._select_explicit_node_id()
        elif self.selecton_type == "all_nodes":
            _init_selected_nodes_df = self.net.nodes_df
        else:
            raise SelectionFormatError("Doesn't have known node selection type")

        self._selected_nodes_df = _init_selected_nodes_df.dict_query(self.additional_node_selection_dict)
        
        return self._selected_nodes_df

    def _validate_node_selection(self) -> bool:
        """Validates that network has node properties that are specified in selection.

        Returns:
            bool:True if node selection is compatable with network
        """
        
        _node_props = set([
            k 
            for c in NODE_PROJECT_CARD_KEYS 
            for k in self.node_selection_dict.get(c,{}).keys() 
        ])

        _missing_node_props = _node_props - set(self.net.nodes_df.columns) - {"all"}

        if _missing_node_props:
            msg = (
                f"Node selection uses properties not in net.nodes_df:{','.join(_missing_node_props)}"
            )
            WranglerLogger.error(msg)
            raise SelectionError(msg)

        return True

    def _validate_link_selection(self) -> bool:
        """Validates that network has link properties that are specified in selection.

        Args:
            coerce_types: if true, will coerce the types of the link selection values to what 
                they are in the associated network. 

        Returns:
            bool:True if link selection is compatable with network
        """

        _missing_link_props = set(list(self.link_selection_dict.keys())) - set(
            self.net.links_df.columns
        ) - {"all"}

        if _missing_link_props:
            msg = (
                f"Link selection uses properties not in net.links_df:{','.join(_missing_link_props)}"
            )
            WranglerLogger.error(msg)
            raise SelectionError(msg)

        return True

    def validate_selection(self) -> bool:
        """
        Validate that selection_dict is compatible with the network.

        Args:
            selection: selection dictionary to be evaluated

        Returns: boolean value as to whether the selection dictonary is valid.
        """
        _links_valid = self._validate_link_selection()
        _nodes_valid = self._validate_node_selection()

        return _links_valid and _nodes_valid
    
    def _select_explicit_link_id(
        self,
    ):
        """Select links based on a explicit link id in selection_dict."""
        WranglerLogger.info("Selecting using explicit link identifiers.")
        _sel_links_mask = self.net.links_df.isin(self.explicit_link_id_selection_dict).any(axis=1)
        _selected_links_df = self.net.links_df.loc[_sel_links_mask]
        WranglerLogger.debug(f"{len(_selected_links_df)} links selected with explicit links dict: {self.explicit_link_id_selection_dict}")

        # make sure in mode
        _selected_links_df = _selected_links_df.mode_query(modes=self.modes)
        WranglerLogger.debug(f"{len(_selected_links_df)} links selected with modes: {self.modes}")

        if not len(_selected_links_df) :
            WranglerLogger.warning("No links found matching criteria.")

        return _selected_links_df

    def _select_explicit_node_id(
        self,
    ):
        """Select nodes based on a explicit node id in selection_dict."""
        WranglerLogger.info("Selecting using explicit node identifiers.")
        _sel_nodes_mask = self.nodes_df.isin(self.explicit_node_id_selection_dict).any(axis=1)
        _selected_nodes_df = self.net.links_df.loc[_sel_nodes_mask]

        WranglerLogger.debug(f"{len(_selected_nodes_df)} links selected with explicit links dict: {self.explicit_node_id_selection_dict}")

        if not _sel_nodes_mask.any():
            WranglerLogger.warning("No nodes found matching criteria.")

        return _selected_nodes_df
