import pandas as pd

import copy
import hashlib
from typing import Any, Mapping

from ..logger import WranglerLogger
from ..utils import delete_keys_from_dict
from .segment import Segment


class SelectionFormatError(Exception):
    pass


class SelectionError(Exception):
    pass


# Project card keys that are associated with node properties
NODE_PROJECT_CARD_KEYS = ["nodes", "node", "A", "B", "from", "to"]

# Project card keys that are associated with link properties
LINK_PROJECT_CARD_KEYS = [
    "links",
    "link",
]

MODES_TO_NETWORK_NODE_VARIABLES = {
    "drive": ["drive_node"],
    "rail": ["rail_only", "drive_node"],
    "bus": ["bus_only", "drive_node"],
    "transit": ["bus_only", "rail_only", "drive_node"],
    "walk": ["walk_node"],
    "bike": ["bike_node"],
}

class RoadwaySelection:
    """_summary_

    Properties:
        net:
        id:
        type: one of "unique_link_id", "unique_node_id" or "segment_search" or "all_links"

    """

    def __init__(
        self,
        net: "RoadwayNetwork",
        selection_dict: dict,
        additional_requirements={"drive_access": True},
        ignore=[],
    ):
        self.net = net

        # This should make it compatible with old and new project card types
        self.selection_dict = selection_dict.update(selection_dict.get("links", {}))
        self.sel_key = RoadwaySelection._assign_selection_key(self.selection_dict)
        self._net_hash = hash(tuple(net.links_df, net.nodes_df))
        self.type = self.get_selection_type(self.selection_dict, self.net)
        self.ignore = ignore + ["O", "D", "A", "B"]

        self.selected_links_df = None
        self._segment = None

        if additional_requirements:
            for k, v in additional_requirements.items():
                self.selection_dict[k] = v

        self.validate_to_net()

    @property
    def selected_links(self) -> list:
        if not self.found:
            return None
        return self.selected_links_df.index.tolist()

    @property
    def found(self) -> bool:
        if self.selected_links_df is not None:
            return True
        return False

    @property
    def segment(self):
        if self._segment is None and self.type == "segment_search":
            self._segment = Segment(self.net, self)
        return self._segment

    @property
    def _node_selection_props(self) -> list:
        """List of properties nodes are selected by."""

        _props = []
        for i in NODE_PROJECT_CARD_KEYS:
            if i not in self.selection_dict:
                continue
            if isinstance(i, dict):
                _props += list(i.keys())
            if isinstance(i, list):
                _props += [x for d in i for x in d.keys()]
        return list(set(_props))

    @property
    def _link_selection_props(self) -> list:
        """List of properties nodes are selected by."""
        _props = []
        _link_keys = LINK_PROJECT_CARD_KEYS + [
            k for k in self.selection_dict.keys() if k not in NODE_PROJECT_CARD_KEYS
        ]
        for i in _link_keys:
            if i not in self.selection_dict:
                continue
            if isinstance(i, dict):
                _props += list(i.keys())
            if isinstance(i, list):
                _props += [x for d in i for x in d.keys()]
        return list(set(_props))

    @staticmethod
    def _assign_selection_key(selection_dict: dict) -> tuple:
        """
        Selections are stored by a hash of the selection dictionary.

        Args:
            selection_dictonary: Selection Dictionary

        Returns: Hex code for hash
        """
        return hashlib.md5(b"selection_dict").hexdigest()

    @staticmethod
    def get_selection_type(selection_dict: dict, net) -> str:
        """Determines what type of selection it is based on format of selection dict.

        Args:
            selection_dict (dict): Selection dictionary from project card.
            net: Roadway Network for selection

        Returns:
            str: Selection type value
        """
        selection_keys = list(selection_dict.keys())

        # if selection has a unique id, then its a unique_id type
        if not set(net.links_df.params.unique_ids).isdisjoint(selection_keys):
            return "unique_link_id"

        elif set(["A", "B", "name"]).issubset(selection_keys):
            return "segment_search"

        elif set(["O", "D", "name"]).issubset(selection_keys):
            return "segment_search"

        elif selection_dict.get("links") == "all":
            return "all_links"

        else:
            WranglerLogger.error(f"Selection type not found for : {selection_dict}")
            WranglerLogger.error(
                f"Expected one of: {net.links_df.params.unique_ids} or A, B, name or O, D, name"
            )
            raise SelectionFormatError(
                "Don't believe selection is valid - can't find a unique id or A, B, \
                    Name or O,D name"
            )

    def select_links(self):
        if self.selection_type == "unique_link_id":
            _initial_selected_links_df = self._select_unique_link_id()
        elif self.selection_type == "segment_search":
            _initial_selected_links_df = copy.deepcopy(self.segment.segment_links_df)
        elif self.selecton_type == "all_links":
            _initial_selected_links_df = copy.deepcopy(self.net.links_df)
        else:
            raise SelectionFormatError("Doesn't have known link selection type")

        self.selected_links_df = copy.deepcopy(
            _initial_selected_links_df.dict_query(self.selection_dict)
        )

    def _validate_node_selection(self) -> bool:
        """Validates that network has node properties that are specified in selection.

        Returns:
            bool:True if node selection is compatable with network
        """

        if self.selection_dict.get("nodes") == "all":
            return True

        _missing_node_props = set(self._node_selection_properties) - set(
            self.net.nodes_df.columns
        )

        if _missing_node_props:
            msg = (
                f"Node selection uses properties not in:{','.join(_missing_node_props)}"
            )
            WranglerLogger.error(msg)
            raise SelectionError(msg)

        return True

    def _validate_link_selection(self) -> bool:
        """Validates that network has link properties that are specified in selection.

        Returns:
            bool:True if link selection is compatable with network
        """
        if self.selection_dict.get("links") == "all":
            return True

        _missing_link_props = set(self._link_selection_properties) - set(
            self.net.links_df.columns
        )

        if _missing_link_props:
            msg = (
                f"Link selection uses properties not in:{','.join(_missing_link_props)}"
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
        return self._validate_link_selection() and self._validate_node_selection()

    def _select_unique_link_id(
        self,
    ):
        """Select links based on a unique link id in selection_dict."""

        _sel_links_mask = self.links_df.isin(self.selection_dict).any(axis=1)
        self.selected_links_df = self.net.links_df.loc[_sel_links_mask]

        if not _sel_links_mask.any():
            WranglerLogger.warning("No links found matching criteria.")
            return False

        return True

    def _property_selection_dict(self, selection_dict):
        """Takes a selection dictionary and returns it with only property-based selections.

        Args:
            selection_dict: original selection dictionary
        """
        NOT_PROPS = (
            ["name", "ref", "all"]
            + self.net.links_df.params.unique_ids
            + self.net.nodes_df.params.unique_ids
        )
        property_selection_dict = delete_keys_from_dict(selection_dict, NOT_PROPS)
        WranglerLogger.debug(f"Property selection dict:{property_selection_dict}")
        return property_selection_dict

