"""Roadway selection classes for selecting links and nodes from a roadway network."""

from __future__ import annotations

import copy
import hashlib

from typing import TYPE_CHECKING, Union

import pandas as pd

from ..logger import WranglerLogger

from ..models.projects.roadway_selection import (
    SelectFacility,
    SelectNodesDict,
    SelectLinksDict,
)
from ..utils.models import (
    coerce_extra_fields_to_type_in_df,
    DatamodelDataframeIncompatableError,
)
from .segment import Segment
from .links.filters import filter_links_to_modes


if TYPE_CHECKING:
    from pandera.typing import DataFrame
    from .network import RoadwayNetwork
    from ..models.roadway.tables import RoadwayLinksTable, RoadwayNodeTable


class SelectionFormatError(Exception):
    """Raised when there is an issue with the format of a selection."""

    pass


class SelectionError(Exception):
    """Raised when there is an issue with a selection."""

    pass


class RoadwayLinkSelection:
    """Object to perform and store information about selection from a project card `facility` key.

    Properties:
        net (RoadwayNetwork): roadway network related to the selection
        raw_selection_dictionary (dict): raw selection dictionary as input to the object used to
            generate `sel_key`.
        sel_key (str): unique selection key based on selection dictionary
        selection_dictionary (dict): dictionary conforming to `SelectFacility` model
            with a `links` key
        selection_data (SelectFacility): selection dictionary as a SelectFacility model
        selection_types (str): one of `explicit_ids`, `segment` or `all`.
            From selection dictionary.
        feature_types (str): returns `links`.  From selection dictionary.
        ignore_missing (bool): True if missing links should be ignored. From selection dictionary.
        modes (list[str]): list of modes that to search for links for. From selection dictionary.
            "any" will return all links without filtering modes.
        explicit_id_sel_dict (dict): explicit link selection criteria. From selection dictionary.
        additional_sel_dict: link selection criteria that is layered on top of a segment, all,
            or explict ID search (i.e. "lanes": [1,2,3], "drive_access": True).  From selection
            dictionary.
        found (bool): True if links were found
        selected_links (list): list of selected link ids
        selected_links_df (DataFrame[RoadwayLinksTable]): lazily-evaluated selected links.
        segment (Segment): segment object if selection type is segment. Defaults to None if not a
            segment selection.
    """

    def __init__(
        self,
        net: RoadwayNetwork,
        selection_data: Union[SelectFacility, dict],
    ):
        """Constructor for RoadwayLinkSelection object.

        Args:
            net (RoadwayNetwork): Roadway network object to select from.
            selection_data: dictionary conforming to
                `SelectFacility` model with a "links" key or SelectFacility instance.
        """
        self.net = net

        WranglerLogger.debug(f"Creating selection from selection dictionary: \n {selection_data}")

        # Coerce the selection dictionary to model types; fill unspecified with default params
        self.selection_dict = selection_data

        self._selected_links_df = None
        self._segment = None

        WranglerLogger.debug(f"Created LinkSelection of type: {self.selection_type}")

    def __nonzero__(self) -> bool:
        """Return True if links were selected."""
        return len(self.selected_links) > 0

    @property
    def feature_types(self):
        """Return the feature type of the selection. Always returns `links`."""
        return "links"

    @property
    def raw_selection_dict(self) -> dict:
        """Raw selection dictionary as input into `net.get_selection()`.

        Used for generating `self.sel_key` and then fed into SelectLinksDict data model where
        additional defaults are set.
        """
        return self._raw_selection_dict

    @raw_selection_dict.setter
    def raw_selection_dict(self, selection_dict: dict):
        self._raw_selection_dict = selection_dict
        self._selection_key = _create_selection_key(selection_dict)

    @property
    def sel_key(self):
        """Return the selection key as generated from `self.raw_selection_dict`."""
        return self._sel_key

    @property
    def selection_data(self):
        """Link selection data from SelectLinksDict."""
        return self._selection_data

    @property
    def selection_type(self):
        """Link selection type from SelectLinksDict: either `all`, `explicit_ids`, or `segment`."""
        return self.selection_data.selection_type

    @property
    def selection_dict(self) -> dict:
        """Selection dictionary dictating the selection settings."""
        return self._selection_dict

    @selection_dict.setter
    def selection_dict(self, selection_input: Union[SelectFacility, dict]):
        if isinstance(selection_input, SelectLinksDict):
            selection_input = SelectFacility(links=selection_input)
        elif isinstance(selection_input, SelectNodesDict):
            selection_input = SelectFacility(nodes=selection_input)

        if isinstance(selection_input, SelectFacility):
            self.raw_selection_dict = selection_input.model_dump(exclude_none=True, by_alias=True)
        else:
            self.raw_selection_dict = selection_input

        if not isinstance(selection_input, SelectFacility):
            self._selection_data = self.validate_selection(SelectFacility(**selection_input))
        else:
            self._selection_data = self.validate_selection(selection_input)

        self._selection_dict = self._selection_data.model_dump(exclude_none=True, by_alias=True)
        self._stored_net_hash = copy.deepcopy(self.net.network_hash)

    @property
    def ignore_missing(self) -> bool:
        """True if missing links should be ignored. From selection dictionary."""
        return self.selection_data.links.ignore_missing

    @property
    def modes(self) -> list[str]:
        """List of modes to search for links for. From selection dictionary.

        `any` will return all links without filtering modes.
        """
        return self.selection_data.links.modes

    @property
    def explicit_id_sel_dict(self):
        """Return a dictionary of fields that are explicit ids. From selection dictionary."""
        return self.selection_data.links.explicit_id_selection_dict

    @property
    def additional_sel_dict(self):
        """Return a dictionary of fields that are not part of the initial selection fields.

        From selection dictionary.
        """
        return self.selection_data.links.additional_selection_dict

    @property
    def selected_links(self) -> list[int]:
        """List of selected link ids."""
        if self.selected_links_df is None:
            return []
        return self.selected_links_df.index.tolist()

    @property
    def found(self) -> bool:
        """Return True if links were found."""
        return self.selected_links_df is not None

    @property
    def segment(self):
        """Return the segment object if selection type is segment."""
        if self._segment is None and self.selection_type == "segment":
            WranglerLogger.debug("Creating new segment")
            self._segment = Segment(self.net, self)
        return self._segment

    @property
    def selected_links_df(self) -> DataFrame[RoadwayLinksTable]:
        """Lazily evaluates selection for links or returns stored value in self._selected_links_df.

        Will re-evaluate if the current network hash is different than the stored one from the
        last selection.
        """
        if self._selected_links_df is None or self._stored_net_hash != self.net.network_hash:
            self._stored_net_hash = copy.deepcopy(self.net.network_hash)
            self._selected_links_df = self._perform_selection()

        return self._selected_links_df

    def validate_selection(self, sel_data: SelectFacility) -> SelectFacility:
        """Validates that selection_dict is compatible with the network."""
        if sel_data.links is None:
            raise SelectionFormatError("Link Selection does not contain links field.")
        try:
            sel_data.links = coerce_extra_fields_to_type_in_df(
                sel_data.links, SelectLinksDict, self.net.links_df
            )
        except DatamodelDataframeIncompatableError as e:
            WranglerLogger.error(f"Invalid link selection fields: {e}")
            raise e
        return sel_data

    def _perform_selection(self):
        # 1. Initial selection based on selection type
        WranglerLogger.debug(
            f"Initial link selection type: \
                             {self.feature_types}.{self.selection_data.selection_type}"
        )
        if self.selection_type == "explicit_ids":
            _selected_links_df = self._select_explicit_link_id()

        elif self.selection_type == "segment":
            _selected_links_df = self.segment.segment_links_df

        elif self.selection_data.selection_type == "all":
            _selected_links_df = self.net.links_df

        else:
            raise SelectionFormatError("Doesn't have known link selection type")

        # 2. Mode selection
        _selected_links_df = filter_links_to_modes(
            _selected_links_df, self.selection_data.links.modes
        )

        # 3. Additional attributes within initial selection
        if self.additional_sel_dict:
            WranglerLogger.debug(f"Initially selected links: {len(_selected_links_df)}")
            if len(_selected_links_df) < 10:
                _cols = _selected_links_df.__dict__["params"].display_cols
                WranglerLogger.debug(f"\n{_selected_links_df[_cols]}")
            WranglerLogger.debug(f"Selecting from selection based on: {self.additional_sel_dict}")
            _selected_links_df = _selected_links_df.dict_query(self.additional_sel_dict)

        if not len(_selected_links_df):
            WranglerLogger.warning("No links found matching criteria.")

        WranglerLogger.info(f"Final selected links: {len(_selected_links_df)}")
        if len(_selected_links_df) < 10:
            _cols = _selected_links_df.__dict__["params"].display_cols
            WranglerLogger.debug(f"\n{_selected_links_df[_cols]}")

        return _selected_links_df

    def _select_explicit_link_id(self):
        """Select links based on a explicit link id in selection_dict."""
        WranglerLogger.info("Selecting using explicit link identifiers.")
        WranglerLogger.debug(f"Explicit link selection dictionary: {self.explicit_id_sel_dict}")
        missing_values = {
            col: list(set(values) - set(self.net.links_df[col]))
            for col, values in self.explicit_id_sel_dict.items()
        }
        missing_df = pd.DataFrame(missing_values)
        if len(missing_df) > 0:
            WranglerLogger.warning(f"Missing explicit link selections: \n{missing_df}")
            if not self.ignore_missing:
                raise SelectionError("Missing explicit link selections.")

        _sel_links_mask = self.net.links_df.isin(self.explicit_id_sel_dict).any(axis=1)
        _sel_links_df = self.net.links_df.loc[_sel_links_mask]

        return _sel_links_df


class RoadwayNodeSelection:
    """Object to perform and store information about node selection from a project card "facility".

    Properties:
        net (RoadwayNetwork): roadway network related to the selection
        raw_selection_dictionary (dict): raw selection dictionary as input to the object used to
            generate `sel_key`.
        sel_key (str): unique selection key based on selection dictionary
        selection_dictionary (dict): dictionary conforming to `SelectFacility` model
            with a `nodes` key
        selection_data (SelectFacility): selection dictionary as a SelectFacility model
        selection_types (str): one of `explicit_ids`, `segment` or `all`.
            From selection dictionary.
        feature_types (str): returns `nodes`.  From selection dictionary.
        ignore_missing (bool): True if missing links should be ignored. From selection dictionary.
        explicit_id_sel_dict (dict): explicit link selection criteria. From selection dictionary.
        found (bool): True if nodes were found
        selected_nodes (list[int]): list of selected nodes ids
        selected_nodes_df (DataFrame[RoadwayNodesTable]): lazily-evaluated selected nodes.
    """

    def __init__(
        self,
        net: RoadwayNetwork,
        selection_data: Union[dict, SelectFacility],
    ):
        """Constructor for RoadwayNodeSelection object.

        Args:
            net (RoadwayNetwork): Roadway network object to select from.
            selection_data (Union[dict, SelectFacility]): Selection dictionary with "nodes" key
                conforming to SelectFacility format, or SelectFacility instance.
        """
        self.net = net

        WranglerLogger.debug(f"Creating selection from selection data: \n{selection_data}")
        # Coerce the selection dictionary to model types; fill unspecified with default params
        self.selection_dict = selection_data

        self._selected_nodes_df = None
        self._segment = None

        WranglerLogger.debug(f"Created NodeSelection of type: {self.selection_type}")

    def __nonzero__(self) -> bool:
        """Return True if nodes were selected."""
        return len(self.selected_nodes) > 0

    @property
    def sel_key(self):
        """Return the selection key as generated from `self.raw_selection_dict`."""
        return self._sel_key

    @property
    def feature_types(self):
        """Return the feature type of the selection. Always returns `nodes`."""
        return "nodes"

    @property
    def selection_data(self):
        """Node selection data from SelectNodesDict."""
        return self._selection_data

    @property
    def selection_type(self):
        """Node selection type from SelectNodesDict: either `all` or `explicit_ids`."""
        return self.selection_data.nodes.selection_type

    @property
    def raw_selection_dict(self) -> dict:
        """Raw selection dictionary as input into `net.get_selection()`.

        Used for generating `self.sel_key` and then fed into SelectNodesDict data model where
        additional defaults are set.
        """
        return self._raw_selection_dict

    @raw_selection_dict.setter
    def raw_selection_dict(self, selection_dict: dict):
        self._raw_selection_dict = selection_dict
        self._selection_key = _create_selection_key(selection_dict)

    @property
    def selection_dict(self) -> dict:
        """Selection dictionary dictating the selection settings."""
        return self._selection_dict

    @selection_dict.setter
    def selection_dict(self, selection_input: Union[dict, SelectFacility]):
        if isinstance(selection_input, SelectFacility):
            self.raw_selection_dict = selection_input.model_dump(exclude_none=True, by_alias=True)
        else:
            self.raw_selection_dict = selection_input

        if not isinstance(selection_input, SelectFacility):
            self._selection_data = self.validate_selection(SelectFacility(**selection_input))
        else:
            self._selection_data = self.validate_selection(selection_input)

        self._selection_dict = self._selection_data.model_dump(exclude_none=True, by_alias=True)
        self._stored_net_hash = copy.deepcopy(self.net.network_hash)

    @property
    def ignore_missing(self) -> bool:
        """True if missing nodes should be ignored."""
        return self.selection_data.nodes.ignore_missing

    @property
    def explicit_id_sel_dict(self) -> dict:
        """Return a dictionary of field that are explicit ids."""
        return self.selection_data.nodes.explicit_id_selection_dict

    @property
    def selected_nodes(self) -> list[int]:
        """List of selected node ids."""
        if self.selected_nodes_df is None:
            return []
        return self.selected_nodes_df.index.tolist()

    @property
    def found(self) -> bool:
        """True if nodes were found."""
        return self.selected_nodes_df is not None

    @property
    def selected_nodes_df(self) -> DataFrame[RoadwayNodeTable]:
        """Lazily evaluates selection for nodes or returns stored value in self._selected_nodes_df.

        Will re-evaluate if the current network hash is different than the stored one from the
        last selection.
        """
        if self._selected_nodes_df is None or self._stored_net_hash != self.net.network_hash:
            self._stored_net_hash = self.net.network_hash
            self._selected_nodes_df = self._perform_selection()

        return self._selected_nodes_df

    def validate_selection(self, selection_data: SelectFacility) -> SelectFacility:
        """Validate that selection_dict is compatible with the network."""
        if selection_data.nodes is None:
            raise SelectionFormatError("Node Selection does not contain nodes field.")

        try:
            selection_data.nodes = coerce_extra_fields_to_type_in_df(
                selection_data.nodes, SelectNodesDict, self.net.nodes_df
            )
        except DatamodelDataframeIncompatableError as e:
            WranglerLogger.error(f"Invalid node selection fields: {e}")
            raise e

        return selection_data

    def _perform_selection(self):
        if self.selection_type == "explicit_ids":
            _selected_nodes_df = self._select_explicit_node_id()
        elif self.selection_type == "all":
            _selected_nodes_df = self.net.nodes_df
        else:
            WranglerLogger.error(f"Didn't understand selection type: {self.selection_type}")
            raise SelectionFormatError("Doesn't have known node selection type")

        WranglerLogger.info(f"Final selected nodes: {len(_selected_nodes_df)}")
        if len(_selected_nodes_df) < 10:
            _cols = _selected_nodes_df.__dict__["params"].display_cols
            WranglerLogger.debug(f"\n{_selected_nodes_df[_cols]}")

        return _selected_nodes_df

    def _select_explicit_node_id(
        self,
    ):
        """Select nodes based on a explicit node id in selection_dict."""
        WranglerLogger.info("Selecting using explicit node identifiers.")

        missing_values = {
            col: list(set(values) - set(self.net.nodes_df[col]))
            for col, values in self.explicit_id_sel_dict.items()
        }
        missing_df = pd.DataFrame(missing_values)
        if len(missing_df) > 0:
            WranglerLogger.warning("Missing explicit node selections:\n{missing_df}")
            if not self.ignore_missing:
                raise SelectionError("Missing explicit node selections.")

        _sel_nodes_mask = self.net.nodes_df.isin(self.explicit_id_sel_dict).any(axis=1)
        _selected_nodes_df = self.net.nodes_df.loc[_sel_nodes_mask]

        return _selected_nodes_df


def _create_selection_key(
    selection_dict: Union[SelectLinksDict, SelectNodesDict, SelectFacility, dict],
) -> str:
    """Selections are stored by a sha1 hash of the bit-encoded string of the selection dictionary.

    Args:
        selection_dict: Selection Dictionary

    Returns: Hex code for hash
    """
    if isinstance(selection_dict, SelectLinksDict):
        selection_dict = SelectFacility(links=selection_dict)
    elif isinstance(selection_dict, SelectNodesDict):
        selection_dict = SelectFacility(nodes=selection_dict)

    if isinstance(selection_dict, SelectFacility):
        selection_dict = selection_dict.model_dump(exclude_none=True, by_alias=True)
    elif not isinstance(selection_dict, dict):
        WranglerLogger.error(f"`selection_dict` arg must be a dictionary or SelectFacility model.\
                             Received: {selection_dict} of type {type(selection_dict)}")
        raise SelectionError("selection_dict arg must be a dictionary or SelectFacility model")
    return hashlib.sha1(str(selection_dict).encode()).hexdigest()
